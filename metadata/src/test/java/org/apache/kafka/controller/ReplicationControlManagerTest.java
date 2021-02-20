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

package org.apache.kafka.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.apache.kafka.common.protocol.Errors.INVALID_TOPIC_EXCEPTION;
import static org.apache.kafka.controller.BrokersToIsrs.TopicPartition;
import static org.apache.kafka.controller.ReplicationControlManager.PartitionControlInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@Timeout(40)
public class ReplicationControlManagerTest {
    private static ReplicationControlManager newReplicationControlManager() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        LogContext logContext = new LogContext();
        MockTime time = new MockTime();
        MockRandom random = new MockRandom();
        ClusterControlManager clusterControl = new ClusterControlManager(
            logContext, time, snapshotRegistry, 1000,
            new SimpleReplicaPlacementPolicy(random));
        clusterControl.activate();
        ConfigurationControlManager configurationControl = new ConfigurationControlManager(
            new LogContext(), snapshotRegistry, Collections.emptyMap());
        return new ReplicationControlManager(snapshotRegistry,
            new LogContext(),
            random,
            (short) 3,
            1,
            configurationControl,
            clusterControl);
    }

    private static void registerBroker(int brokerId, ClusterControlManager clusterControl) {
        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().
            setBrokerEpoch(100).setBrokerId(brokerId);
        brokerRecord.endPoints().add(new RegisterBrokerRecord.BrokerEndpoint().
            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
            setPort((short) 9092 + brokerId).
            setName("PLAINTEXT").
            setHost("localhost"));
        clusterControl.replay(brokerRecord);
    }

    private static void unfenceBroker(int brokerId,
                                      ReplicationControlManager replicationControl) throws Exception {
        ControllerResult<BrokerHeartbeatReply> result = replicationControl.
            processBrokerHeartbeat(new BrokerHeartbeatRequestData().
                setBrokerId(brokerId).setBrokerEpoch(100).setCurrentMetadataOffset(1).
                setWantFence(false).setWantShutDown(false), 0);
        assertEquals(new BrokerHeartbeatReply(true, false, false, false), result.response());
        ControllerTestUtils.replayAll(replicationControl.clusterControl, result.records());
    }

    @Test
    public void testCreateTopics() throws Exception {
        ReplicationControlManager replicationControl = newReplicationControlManager();
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(-1).setReplicationFactor((short) -1));
        ControllerResult<CreateTopicsResponseData> result =
            replicationControl.createTopics(request);
        CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
        expectedResponse.topics().add(new CreatableTopicResult().setName("foo").
            setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code()).
                setErrorMessage("Unable to replicate the partition 3 times: there are only 0 usable brokers"));
        assertEquals(expectedResponse, result.response());

        registerBroker(0, replicationControl.clusterControl);
        unfenceBroker(0, replicationControl);
        registerBroker(1, replicationControl.clusterControl);
        unfenceBroker(1, replicationControl);
        registerBroker(2, replicationControl.clusterControl);
        unfenceBroker(2, replicationControl);
        ControllerResult<CreateTopicsResponseData> result2 =
            replicationControl.createTopics(request);
        CreateTopicsResponseData expectedResponse2 = new CreateTopicsResponseData();
        expectedResponse2.topics().add(new CreatableTopicResult().setName("foo").
            setNumPartitions(1).setReplicationFactor((short) 3).
            setErrorMessage(null).setErrorCode((short) 0).
            setTopicId(result2.response().topics().find("foo").topicId()));
        assertEquals(expectedResponse2, result2.response());
        ControllerTestUtils.replayAll(replicationControl, result2.records());
        assertEquals(new PartitionControlInfo(new int[] {2, 0, 1},
            new int[] {2, 0, 1}, null, null, 2, 0, 0),
            replicationControl.getPartition(
                ((TopicRecord) result2.records().get(0).message()).topicId(), 0));
        ControllerResult<CreateTopicsResponseData> result3 =
                replicationControl.createTopics(request);
        CreateTopicsResponseData expectedResponse3 = new CreateTopicsResponseData();
        expectedResponse3.topics().add(new CreatableTopicResult().setName("foo").
                setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code()).
                setErrorMessage(Errors.TOPIC_ALREADY_EXISTS.exception().getMessage()));
        assertEquals(expectedResponse3, result3.response());
    }

    @Test
    public void testValidateNewTopicNames() {
        Map<String, ApiError> topicErrors = new HashMap<>();
        CreatableTopicCollection topics = new CreatableTopicCollection();
        topics.add(new CreatableTopic().setName(""));
        topics.add(new CreatableTopic().setName("woo"));
        topics.add(new CreatableTopic().setName("."));
        ReplicationControlManager.validateNewTopicNames(topicErrors, topics);
        Map<String, ApiError> expectedTopicErrors = new HashMap<>();
        expectedTopicErrors.put("", new ApiError(INVALID_TOPIC_EXCEPTION,
            "Topic name is illegal, it can't be empty"));
        expectedTopicErrors.put(".", new ApiError(INVALID_TOPIC_EXCEPTION,
            "Topic name cannot be \".\" or \"..\""));
        assertEquals(expectedTopicErrors, topicErrors);
    }

    private static CreatableTopicResult createTestTopic(
            ReplicationControlManager replicationControl, String name,
            int[][] replicas) throws Exception {
        assertFalse(replicas.length == 0);
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        CreatableTopic topic = new CreatableTopic().setName(name);
        topic.setNumPartitions(-1).setReplicationFactor((short) -1);
        for (int i = 0; i < replicas.length; i++) {
            topic.assignments().add(new CreatableReplicaAssignment().
                setPartitionIndex(i).setBrokerIds(Replicas.toList(replicas[i])));
        }
        request.topics().add(topic);
        ControllerResult<CreateTopicsResponseData> result =
            replicationControl.createTopics(request);
        CreatableTopicResult topicResult = result.response().topics().find(name);
        assertNotNull(topicResult);
        assertEquals((short) 0, topicResult.errorCode());
        assertEquals(replicas.length, topicResult.numPartitions());
        assertEquals(replicas[0].length, topicResult.replicationFactor());
        ControllerTestUtils.replayAll(replicationControl, result.records());
        return topicResult;
    }

    @Test
    public void testRemoveLeaderships() throws Exception {
        ReplicationControlManager replicationControl = newReplicationControlManager();
        for (int i = 0; i < 6; i++) {
            registerBroker(i, replicationControl.clusterControl);
            unfenceBroker(i, replicationControl);
        }
        CreatableTopicResult result = createTestTopic(replicationControl, "foo",
            new int[][] {
                new int[] {0, 1, 2},
                new int[] {1, 2, 3},
                new int[] {2, 3, 0},
                new int[] {0, 2, 1}
            });
        Set<TopicPartition> expectedPartitions = new HashSet<>();
        expectedPartitions.add(new TopicPartition(result.topicId(), 0));
        expectedPartitions.add(new TopicPartition(result.topicId(), 3));
        assertEquals(expectedPartitions, ControllerTestUtils.
            iteratorToSet(replicationControl.brokersToIsrs().iterator(0, true)));
        List<ApiMessageAndVersion> records = new ArrayList<>();
        replicationControl.handleNodeDeactivated(0, records);
        ControllerTestUtils.replayAll(replicationControl, records);
        assertEquals(Collections.emptySet(), ControllerTestUtils.
            iteratorToSet(replicationControl.brokersToIsrs().iterator(0, true)));
    }
}
