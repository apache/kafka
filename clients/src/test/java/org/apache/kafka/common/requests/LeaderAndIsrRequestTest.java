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
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrLiveLeader;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageTestUtil;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.kafka.common.protocol.ApiKeys.LEADER_AND_ISR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LeaderAndIsrRequestTest {

    @Test
    public void testUnsupportedVersion() {
        LeaderAndIsrRequest.Builder builder = new LeaderAndIsrRequest.Builder(0, 0, 0,
                Collections.emptyList(), Collections.emptySet());
        assertThrows(UnsupportedVersionException.class, () -> builder.build((short) (LEADER_AND_ISR.latestVersion() + 1)));
    }

    @Test
    public void testGetErrorResponse() {
        for (short version = LEADER_AND_ISR.oldestVersion(); version < LEADER_AND_ISR.latestVersion(); version++) {
            LeaderAndIsrRequest.Builder builder = new LeaderAndIsrRequest.Builder(0, 0, 0,
                    Collections.emptyList(), Collections.emptySet());
            LeaderAndIsrRequest request = builder.build(version);
            LeaderAndIsrResponse response = request.getErrorResponse(0,
                    new ClusterAuthorizationException("Not authorized"));
            assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error());
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
        for (short version = LEADER_AND_ISR.oldestVersion(); version <= LEADER_AND_ISR.latestVersion(); version++) {
            List<LeaderAndIsrPartitionState> partitionStates = asList(
                new LeaderAndIsrPartitionState()
                    .setTopicName("topic0")
                    .setPartitionIndex(0)
                    .setControllerEpoch(2)
                    .setLeader(0)
                    .setLeaderEpoch(10)
                    .setIsr(asList(0, 1))
                    .setZkVersion(10)
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
                    .setZkVersion(11)
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
                    .setZkVersion(11)
                    .setReplicas(asList(2, 3, 4))
                    .setAddingReplicas(emptyList())
                    .setRemovingReplicas(emptyList())
            );

            List<Node> liveNodes = asList(
                new Node(0, "host0", 9090),
                new Node(1, "host1", 9091)
            );
            LeaderAndIsrRequest request = new LeaderAndIsrRequest.Builder(1, 2, 3, partitionStates,
                liveNodes).build(version);

            List<LeaderAndIsrLiveLeader> liveLeaders = liveNodes.stream().map(n -> new LeaderAndIsrLiveLeader()
                .setBrokerId(n.id())
                .setHostName(n.host())
                .setPort(n.port())).collect(Collectors.toList());
            assertEquals(new HashSet<>(partitionStates), iterableToSet(request.partitionStates()));
            assertEquals(liveLeaders, request.liveLeaders());
            assertEquals(1, request.controllerId());
            assertEquals(2, request.controllerEpoch());
            assertEquals(3, request.brokerEpoch());

            ByteBuffer byteBuffer = MessageTestUtil.messageToByteBuffer(request.data(), request.version());
            LeaderAndIsrRequest deserializedRequest = new LeaderAndIsrRequest(new LeaderAndIsrRequestData(
                new ByteBufferAccessor(byteBuffer), version), version);

            // Adding/removing replicas is only supported from version 3, so the deserialized request won't have
            // them for earlier versions.
            if (version < 3) {
                partitionStates.get(0)
                    .setAddingReplicas(emptyList())
                    .setRemovingReplicas(emptyList());
            }

            assertEquals(new HashSet<>(partitionStates), iterableToSet(deserializedRequest.partitionStates()));
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
        for (TopicPartition tp : tps) {
            partitionStates.add(new LeaderAndIsrPartitionState()
                .setTopicName(tp.topic())
                .setPartitionIndex(tp.partition()));
        }
        LeaderAndIsrRequest.Builder builder = new LeaderAndIsrRequest.Builder(0, 0, 0,
            partitionStates, Collections.emptySet());

        LeaderAndIsrRequest v2 = builder.build((short) 2);
        LeaderAndIsrRequest v1 = builder.build((short) 1);
        int size2 = MessageTestUtil.messageSize(v2.data(), v2.version());
        int size1 = MessageTestUtil.messageSize(v1.data(), v1.version());

        assertTrue("Expected v2 < v1: v2=" + size2 + ", v1=" + size1, size2 < size1);
    }

    private <T> Set<T> iterableToSet(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toSet());
    }
}
