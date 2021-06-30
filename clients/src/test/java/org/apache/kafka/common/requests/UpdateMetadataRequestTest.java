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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataEndpoint;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.SecurityProtocol;
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
import static org.apache.kafka.common.protocol.ApiKeys.UPDATE_METADATA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UpdateMetadataRequestTest {

    @Test
    public void testUnsupportedVersion() {
        UpdateMetadataRequest.Builder builder = new UpdateMetadataRequest.Builder(
                (short) (UPDATE_METADATA.latestVersion() + 1), 0, 0, 0,
                Collections.emptyList(), Collections.emptyList(), Collections.emptyMap());
        assertThrows(UnsupportedVersionException.class, builder::build);
    }

    @Test
    public void testGetErrorResponse() {
        for (short version : UPDATE_METADATA.allVersions()) {
            UpdateMetadataRequest.Builder builder = new UpdateMetadataRequest.Builder(
                    version, 0, 0, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyMap());
            UpdateMetadataRequest request = builder.build();
            UpdateMetadataResponse response = request.getErrorResponse(0,
                    new ClusterAuthorizationException("Not authorized"));
            assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error());
        }
    }

    /**
     * Verifies the logic we have in UpdateMetadataRequest to present a unified interface across the various versions
     * works correctly. For example, `UpdateMetadataPartitionState.topicName` is not serialiazed/deserialized in
     * recent versions, but we set it manually so that we can always present the ungrouped partition states
     * independently of the version.
     */
    @Test
    public void testVersionLogic() {
        String topic0 = "topic0";
        String topic1 = "topic1";
        for (short version : UPDATE_METADATA.allVersions()) {
            List<UpdateMetadataPartitionState> partitionStates = asList(
                new UpdateMetadataPartitionState()
                    .setTopicName(topic0)
                    .setPartitionIndex(0)
                    .setControllerEpoch(2)
                    .setLeader(0)
                    .setLeaderEpoch(10)
                    .setIsr(asList(0, 1))
                    .setZkVersion(10)
                    .setReplicas(asList(0, 1, 2))
                    .setOfflineReplicas(asList(2)),
                new UpdateMetadataPartitionState()
                    .setTopicName(topic0)
                    .setPartitionIndex(1)
                    .setControllerEpoch(2)
                    .setLeader(1)
                    .setLeaderEpoch(11)
                    .setIsr(asList(1, 2, 3))
                    .setZkVersion(11)
                    .setReplicas(asList(1, 2, 3))
                    .setOfflineReplicas(emptyList()),
                new UpdateMetadataPartitionState()
                    .setTopicName(topic1)
                    .setPartitionIndex(0)
                    .setControllerEpoch(2)
                    .setLeader(2)
                    .setLeaderEpoch(11)
                    .setIsr(asList(2, 3))
                    .setZkVersion(11)
                    .setReplicas(asList(2, 3, 4))
                    .setOfflineReplicas(emptyList())
            );

            List<UpdateMetadataEndpoint> broker0Endpoints = new ArrayList<>();
            broker0Endpoints.add(
                new UpdateMetadataEndpoint()
                    .setHost("host0")
                    .setPort(9090)
                    .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id));

            // Non plaintext endpoints are only supported from version 1
            if (version >= 1) {
                broker0Endpoints.add(new UpdateMetadataEndpoint()
                    .setHost("host0")
                    .setPort(9091)
                    .setSecurityProtocol(SecurityProtocol.SSL.id));
            }

            // Custom listeners are only supported from version 3
            if (version >= 3) {
                broker0Endpoints.get(0).setListener("listener0");
                broker0Endpoints.get(1).setListener("listener1");
            }

            List<UpdateMetadataBroker> liveBrokers = asList(
                new UpdateMetadataBroker()
                    .setId(0)
                    .setRack("rack0")
                    .setEndpoints(broker0Endpoints),
                new UpdateMetadataBroker()
                    .setId(1)
                    .setEndpoints(asList(
                        new UpdateMetadataEndpoint()
                            .setHost("host1")
                            .setPort(9090)
                            .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
                            .setListener("PLAINTEXT")
                    ))
            );

            Map<String, Uuid> topicIds = new HashMap<>();
            topicIds.put(topic0, Uuid.randomUuid());
            topicIds.put(topic1, Uuid.randomUuid());

            UpdateMetadataRequest request = new UpdateMetadataRequest.Builder(version, 1, 2, 3,
                partitionStates, liveBrokers, topicIds).build();

            assertEquals(new HashSet<>(partitionStates), iterableToSet(request.partitionStates()));
            assertEquals(liveBrokers, request.liveBrokers());
            assertEquals(1, request.controllerId());
            assertEquals(2, request.controllerEpoch());
            assertEquals(3, request.brokerEpoch());

            ByteBuffer byteBuffer = request.serialize();
            UpdateMetadataRequest deserializedRequest = new UpdateMetadataRequest(new UpdateMetadataRequestData(
                    new ByteBufferAccessor(byteBuffer), version), version);

            // Unset fields that are not supported in this version as the deserialized request won't have them

            // Rack is only supported from version 2
            if (version < 2) {
                for (UpdateMetadataBroker liveBroker : liveBrokers)
                    liveBroker.setRack("");
            }

            // Non plaintext listener name is only supported from version 3
            if (version < 3) {
                for (UpdateMetadataBroker liveBroker : liveBrokers) {
                    for (UpdateMetadataEndpoint endpoint : liveBroker.endpoints()) {
                        SecurityProtocol securityProtocol = SecurityProtocol.forId(endpoint.securityProtocol());
                        endpoint.setListener(ListenerName.forSecurityProtocol(securityProtocol).value());
                    }
                }
            }

            // Offline replicas are only supported from version 4
            if (version < 4)
                partitionStates.get(0).setOfflineReplicas(emptyList());

            assertEquals(new HashSet<>(partitionStates), iterableToSet(deserializedRequest.partitionStates()));
            assertEquals(liveBrokers, deserializedRequest.liveBrokers());
            assertEquals(1, deserializedRequest.controllerId());
            assertEquals(2, deserializedRequest.controllerEpoch());
            // Broker epoch is only supported from version 5
            if (version >= 5)
                assertEquals(3, deserializedRequest.brokerEpoch());
            else
                assertEquals(-1, deserializedRequest.brokerEpoch());

            long topicIdCount = deserializedRequest.data().topicStates().stream()
                    .map(UpdateMetadataRequestData.UpdateMetadataTopicState::topicId)
                    .filter(topicId -> topicId != Uuid.ZERO_UUID).count();
            if (version >= 7)
                assertEquals(2, topicIdCount);
            else
                assertEquals(0, topicIdCount);
        }
    }

    @Test
    public void testTopicPartitionGroupingSizeReduction() {
        Set<TopicPartition> tps = TestUtils.generateRandomTopicPartitions(10, 10);
        List<UpdateMetadataPartitionState> partitionStates = new ArrayList<>();
        for (TopicPartition tp : tps) {
            partitionStates.add(new UpdateMetadataPartitionState()
                .setTopicName(tp.topic())
                .setPartitionIndex(tp.partition()));
        }
        UpdateMetadataRequest.Builder builder = new UpdateMetadataRequest.Builder((short) 5, 0, 0, 0,
                partitionStates, Collections.emptyList(), Collections.emptyMap());

        assertTrue(builder.build((short) 5).sizeInBytes() <  builder.build((short) 4).sizeInBytes());
    }

    private <T> Set<T> iterableToSet(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toSet());
    }
}
