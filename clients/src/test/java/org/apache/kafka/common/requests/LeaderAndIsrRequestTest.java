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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrLiveLeader;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError;
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrTopicError;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.kafka.common.protocol.ApiKeys.LEADER_AND_ISR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LeaderAndIsrRequestTest {

    @Test
    public void testUnsupportedVersion() {
        LeaderAndIsrRequest.Builder builder = new LeaderAndIsrRequest.Builder(
                (short) (LEADER_AND_ISR.latestVersion() + 1), 0, 0, 0,
                Collections.emptyList(), Collections.emptyMap(), Collections.emptySet());
        assertThrows(UnsupportedVersionException.class, builder::build);
    }

    @Test
    public void testGetErrorResponse() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic";
        int partition = 0;
        for (short version : LEADER_AND_ISR.allVersions()) {
            LeaderAndIsrRequest request = new LeaderAndIsrRequest.Builder(version, 0, 0, 0,
                Collections.singletonList(new LeaderAndIsrPartitionState()
                    .setTopicName(topicName)
                    .setPartitionIndex(partition)),
                Collections.singletonMap(topicName, topicId),
                Collections.emptySet()
            ).build(version);

            LeaderAndIsrResponse response = request.getErrorResponse(0,
                new ClusterAuthorizationException("Not authorized"));

            assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error());

            if (version < 5) {
                assertEquals(
                    Collections.singletonList(new LeaderAndIsrPartitionError()
                        .setTopicName(topicName)
                        .setPartitionIndex(partition)
                        .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())),
                    response.data().partitionErrors());
                assertEquals(0, response.data().topics().size());
            } else {
                LeaderAndIsrTopicError topicState = response.topics().find(topicId);
                assertEquals(topicId, topicState.topicId());
                assertEquals(
                    Collections.singletonList(new LeaderAndIsrPartitionError()
                        .setPartitionIndex(partition)
                        .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())),
                    topicState.partitionErrors());
                assertEquals(0, response.data().partitionErrors().size());
            }
        }
    }

    /**
     * Verifies the logic we have in LeaderAndIsrRequest to present a unified interface across the various versions
     * works correctly. For example, `LeaderAndIsrPartitionState.topicName` is not serialiazed/deserialized in
     * recent versions, but we set it manually so that we can always present the ungrouped partition states
     * independently of the version.
     */
    @Test
    public void testVersionLogic() {
        for (short version : LEADER_AND_ISR.allVersions()) {
            List<LeaderAndIsrPartitionState> partitionStates = asList(
                new LeaderAndIsrPartitionState()
                    .setTopicName("topic0")
                    .setPartitionIndex(0)
                    .setControllerEpoch(2)
                    .setLeader(0)
                    .setLeaderEpoch(10)
                    .setIsr(asList(0, 1))
                    .setPartitionEpoch(10)
                    .setReplicas(asList(0, 1, 2))
                    .setAddingReplicas(asList(3))
                    .setRemovingReplicas(asList(2)),
                new LeaderAndIsrPartitionState()
                    .setTopicName("topic0")
                    .setPartitionIndex(1)
                    .setControllerEpoch(2)
                    .setLeader(1)
                    .setLeaderEpoch(11)
                    .setIsr(asList(1, 2, 3))
                    .setPartitionEpoch(11)
                    .setReplicas(asList(1, 2, 3))
                    .setAddingReplicas(emptyList())
                    .setRemovingReplicas(emptyList()),
                new LeaderAndIsrPartitionState()
                    .setTopicName("topic1")
                    .setPartitionIndex(0)
                    .setControllerEpoch(2)
                    .setLeader(2)
                    .setLeaderEpoch(11)
                    .setIsr(asList(2, 3, 4))
                    .setPartitionEpoch(11)
                    .setReplicas(asList(2, 3, 4))
                    .setAddingReplicas(emptyList())
                    .setRemovingReplicas(emptyList())
            );

            List<Node> liveNodes = asList(
                new Node(0, "host0", 9090),
                new Node(1, "host1", 9091)
            );

            Map<String, Uuid> topicIds = new HashMap<>();
            topicIds.put("topic0", Uuid.randomUuid());
            topicIds.put("topic1", Uuid.randomUuid());

            LeaderAndIsrRequest request = new LeaderAndIsrRequest.Builder(version, 1, 2, 3, partitionStates,
                topicIds, liveNodes).build();

            List<LeaderAndIsrLiveLeader> liveLeaders = liveNodes.stream().map(n -> new LeaderAndIsrLiveLeader()
                .setBrokerId(n.id())
                .setHostName(n.host())
                .setPort(n.port())).collect(Collectors.toList());
            assertEquals(new HashSet<>(partitionStates), iterableToSet(request.partitionStates()));
            assertEquals(liveLeaders, request.liveLeaders());
            assertEquals(1, request.controllerId());
            assertEquals(2, request.controllerEpoch());
            assertEquals(3, request.brokerEpoch());

            ByteBuffer byteBuffer = request.serialize();
            LeaderAndIsrRequest deserializedRequest = new LeaderAndIsrRequest(new LeaderAndIsrRequestData(
                new ByteBufferAccessor(byteBuffer), version), version);

            // Adding/removing replicas is only supported from version 3, so the deserialized request won't have
            // them for earlier versions.
            if (version < 3) {
                partitionStates.get(0)
                    .setAddingReplicas(emptyList())
                    .setRemovingReplicas(emptyList());
            }

            // Prior to version 2, there were no TopicStates, so a map of Topic Ids from a list of
            // TopicStates is an empty map.
            if (version < 2) {
                topicIds = new HashMap<>();
            }

            //  In versions 2-4 there are TopicStates, but no topicIds, so deserialized requests will have
            //  Zero Uuids in place.
            if (version > 1 && version < 5) {
                topicIds.put("topic0", Uuid.ZERO_UUID);
                topicIds.put("topic1", Uuid.ZERO_UUID);
            }

            assertEquals(new HashSet<>(partitionStates), iterableToSet(deserializedRequest.partitionStates()));
            assertEquals(topicIds, deserializedRequest.topicIds());
            assertEquals(liveLeaders, deserializedRequest.liveLeaders());
            assertEquals(1, request.controllerId());
            assertEquals(2, request.controllerEpoch());
            assertEquals(3, request.brokerEpoch());
        }
    }

    @Test
    public void testTopicPartitionGroupingSizeReduction() {
        Set<TopicPartition> tps = TestUtils.generateRandomTopicPartitions(10, 10);
        List<LeaderAndIsrPartitionState> partitionStates = new ArrayList<>();
        Map<String, Uuid> topicIds = new HashMap<>();
        for (TopicPartition tp : tps) {
            partitionStates.add(new LeaderAndIsrPartitionState()
                .setTopicName(tp.topic())
                .setPartitionIndex(tp.partition()));
            topicIds.put(tp.topic(), Uuid.randomUuid());
        }
        LeaderAndIsrRequest.Builder builder = new LeaderAndIsrRequest.Builder((short) 2, 0, 0, 0,
            partitionStates, topicIds, Collections.emptySet());

        LeaderAndIsrRequest v2 = builder.build((short) 2);
        LeaderAndIsrRequest v1 = builder.build((short) 1);
        assertTrue(v2.sizeInBytes() < v1.sizeInBytes(), "Expected v2 < v1: v2=" + v2.sizeInBytes() + ", v1=" + v1.sizeInBytes());
    }

    private <T> Set<T> iterableToSet(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toSet());
    }
}
