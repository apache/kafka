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
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.StopReplicaRequestData;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionV0;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaTopicV1;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaTopicState;
import org.apache.kafka.common.message.StopReplicaResponseData.StopReplicaPartitionError;
import org.apache.kafka.common.protocol.Errors;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.protocol.ApiKeys.STOP_REPLICA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class StopReplicaRequestTest {

    @Test
    public void testUnsupportedVersion() {
        StopReplicaRequest.Builder builder = new StopReplicaRequest.Builder(
                (short) (STOP_REPLICA.latestVersion() + 1),
                0, 0, 0L, false, Collections.emptyList());
        assertThrows(UnsupportedVersionException.class, builder::build);
    }

    @Test
    public void testGetErrorResponse() {
        List<StopReplicaTopicState> topicStates = topicStates(true);

        Set<StopReplicaPartitionError> expectedPartitions = new HashSet<>();
        for (StopReplicaTopicState topicState : topicStates) {
            for (StopReplicaPartitionState partitionState: topicState.partitionStates()) {
                expectedPartitions.add(new StopReplicaPartitionError()
                    .setTopicName(topicState.topicName())
                    .setPartitionIndex(partitionState.partitionIndex())
                    .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code()));
            }
        }

        for (short version = STOP_REPLICA.oldestVersion(); version < STOP_REPLICA.latestVersion(); version++) {
            StopReplicaRequest.Builder builder = new StopReplicaRequest.Builder(version,
                    0, 0, 0L, false, topicStates);
            StopReplicaRequest request = builder.build();
            StopReplicaResponse response = request.getErrorResponse(0,
                    new ClusterAuthorizationException("Not authorized"));
            assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error());
            assertEquals(expectedPartitions, new HashSet<>(response.partitionErrors()));
        }
    }

    @Test
    public void testBuilderNormalizationWithAllDeletePartitionEqualToTrue() {
        testBuilderNormalization(true);
    }

    @Test
    public void testBuilderNormalizationWithAllDeletePartitionEqualToFalse() {
        testBuilderNormalization(false);
    }

    private void testBuilderNormalization(boolean deletePartitions) {
        List<StopReplicaTopicState> topicStates = topicStates(deletePartitions);

        Map<TopicPartition, StopReplicaPartitionState> expectedPartitionStates =
            StopReplicaRequestTest.partitionStates(topicStates);

        for (short version = STOP_REPLICA.oldestVersion(); version < STOP_REPLICA.latestVersion(); version++) {
            StopReplicaRequest request = new StopReplicaRequest.Builder(version, 0, 1, 0,
                deletePartitions, topicStates).build(version);
            StopReplicaRequestData data = request.data();

            if (version < 1) {
                Set<TopicPartition> partitions = new HashSet<>();
                for (StopReplicaPartitionV0 partition : data.ungroupedPartitions()) {
                    partitions.add(new TopicPartition(partition.topicName(), partition.partitionIndex()));
                }
                assertEquals(expectedPartitionStates.keySet(), partitions);
                assertEquals(deletePartitions, data.deletePartitions());
            } else if (version < 3) {
                Set<TopicPartition> partitions = new HashSet<>();
                for (StopReplicaTopicV1 topic : data.topics()) {
                    for (Integer partition : topic.partitionIndexes()) {
                        partitions.add(new TopicPartition(topic.name(), partition));
                    }
                }
                assertEquals(expectedPartitionStates.keySet(), partitions);
                assertEquals(deletePartitions, data.deletePartitions());
            } else {
                Map<TopicPartition, StopReplicaPartitionState> partitionStates =
                    StopReplicaRequestTest.partitionStates(data.topicStates());
                assertEquals(expectedPartitionStates, partitionStates);
                // Always false from V3 on
                assertFalse(data.deletePartitions());
            }
        }
    }

    @Test
    public void testTopicStatesNormalization() {
        List<StopReplicaTopicState> topicStates = topicStates(true);

        for (short version = STOP_REPLICA.oldestVersion(); version < STOP_REPLICA.latestVersion(); version++) {
            // Create a request for version to get its serialized form
            StopReplicaRequest baseRequest = new StopReplicaRequest.Builder(version, 0, 1, 0,
                true, topicStates).build(version);

            // Construct the request from the buffer
            StopReplicaRequest request = StopReplicaRequest.parse(baseRequest.serialize(), version);

            Map<TopicPartition, StopReplicaPartitionState> partitionStates =
                StopReplicaRequestTest.partitionStates(request.topicStates());
            assertEquals(6, partitionStates.size());

            for (StopReplicaTopicState expectedTopicState : topicStates) {
                for (StopReplicaPartitionState expectedPartitionState: expectedTopicState.partitionStates()) {
                    TopicPartition tp = new TopicPartition(expectedTopicState.topicName(),
                        expectedPartitionState.partitionIndex());
                    StopReplicaPartitionState partitionState = partitionStates.get(tp);

                    assertEquals(expectedPartitionState.partitionIndex(), partitionState.partitionIndex());
                    assertTrue(partitionState.deletePartition());

                    if (version >= 3) {
                        assertEquals(expectedPartitionState.leaderEpoch(), partitionState.leaderEpoch());
                    } else {
                        assertEquals(-1, partitionState.leaderEpoch());
                    }
                }
            }
        }
    }

    @Test
    public void testPartitionStatesNormalization() {
        List<StopReplicaTopicState> topicStates = topicStates(true);

        for (short version = STOP_REPLICA.oldestVersion(); version < STOP_REPLICA.latestVersion(); version++) {
            // Create a request for version to get its serialized form
            StopReplicaRequest baseRequest = new StopReplicaRequest.Builder(version, 0, 1, 0,
                true, topicStates).build(version);

            // Construct the request from the buffer
            StopReplicaRequest request = StopReplicaRequest.parse(baseRequest.serialize(), version);

            Map<TopicPartition, StopReplicaPartitionState> partitionStates = request.partitionStates();
            assertEquals(6, partitionStates.size());

            for (StopReplicaTopicState expectedTopicState : topicStates) {
                for (StopReplicaPartitionState expectedPartitionState: expectedTopicState.partitionStates()) {
                    TopicPartition tp = new TopicPartition(expectedTopicState.topicName(),
                        expectedPartitionState.partitionIndex());
                    StopReplicaPartitionState partitionState = partitionStates.get(tp);

                    assertEquals(expectedPartitionState.partitionIndex(), partitionState.partitionIndex());
                    assertTrue(partitionState.deletePartition());

                    if (version >= 3) {
                        assertEquals(expectedPartitionState.leaderEpoch(), partitionState.leaderEpoch());
                    } else {
                        assertEquals(-1, partitionState.leaderEpoch());
                    }
                }
            }
        }
    }

    private List<StopReplicaTopicState> topicStates(boolean deletePartition) {
        List<StopReplicaTopicState> topicStates = new ArrayList<>();
        StopReplicaTopicState topic0 = new StopReplicaTopicState()
            .setTopicName("topic0");
        topic0.partitionStates().add(new StopReplicaPartitionState()
            .setPartitionIndex(0)
            .setLeaderEpoch(0)
            .setDeletePartition(deletePartition));
        topic0.partitionStates().add(new StopReplicaPartitionState()
            .setPartitionIndex(1)
            .setLeaderEpoch(1)
            .setDeletePartition(deletePartition));
        topicStates.add(topic0);
        StopReplicaTopicState topic1 = new StopReplicaTopicState()
            .setTopicName("topic1");
        topic1.partitionStates().add(new StopReplicaPartitionState()
            .setPartitionIndex(2)
            .setLeaderEpoch(2)
            .setDeletePartition(deletePartition));
        topic1.partitionStates().add(new StopReplicaPartitionState()
            .setPartitionIndex(3)
            .setLeaderEpoch(3)
            .setDeletePartition(deletePartition));
        topicStates.add(topic1);
        StopReplicaTopicState topic3 = new StopReplicaTopicState()
            .setTopicName("topic1");
        topic3.partitionStates().add(new StopReplicaPartitionState()
            .setPartitionIndex(4)
            .setLeaderEpoch(-2)
            .setDeletePartition(deletePartition));
        topic3.partitionStates().add(new StopReplicaPartitionState()
            .setPartitionIndex(5)
            .setLeaderEpoch(-2)
            .setDeletePartition(deletePartition));
        topicStates.add(topic3);
        return topicStates;
    }

    public static Map<TopicPartition, StopReplicaPartitionState> partitionStates(
        Iterable<StopReplicaTopicState> topicStates) {
        Map<TopicPartition, StopReplicaPartitionState> partitionStates = new HashMap<>();
        for (StopReplicaTopicState topicState : topicStates) {
            for (StopReplicaPartitionState partitionState: topicState.partitionStates()) {
                partitionStates.put(
                    new TopicPartition(topicState.topicName(), partitionState.partitionIndex()),
                    partitionState);
            }
        }
        return partitionStates;
    }
}
