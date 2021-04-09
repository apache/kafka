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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.apache.kafka.common.message.AlterIsrRequestData;
import org.apache.kafka.common.message.AlterIsrResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.protocol.Errors.INVALID_TOPIC_EXCEPTION;
import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_ID;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION;
import static org.apache.kafka.controller.BrokersToIsrs.TopicIdPartition;
import static org.apache.kafka.controller.ReplicationControlManager.PartitionControlInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class ReplicationControlManagerTest {
    private static class ReplicationControlTestContext {
        final SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        final LogContext logContext = new LogContext();
        final MockTime time = new MockTime();
        final MockRandom random = new MockRandom();
        final ClusterControlManager clusterControl = new ClusterControlManager(
            logContext, time, snapshotRegistry, 1000,
            new SimpleReplicaPlacementPolicy(random));
        final ConfigurationControlManager configurationControl = new ConfigurationControlManager(
            new LogContext(), snapshotRegistry, Collections.emptyMap());
        final ReplicationControlManager replicationControl = new ReplicationControlManager(snapshotRegistry,
            new LogContext(),
            (short) 3,
            1,
            configurationControl,
            clusterControl);

        void replay(List<ApiMessageAndVersion> records) throws Exception {
            ControllerTestUtils.replayAll(clusterControl, records);
            ControllerTestUtils.replayAll(configurationControl, records);
            ControllerTestUtils.replayAll(replicationControl, records);
        }

        ReplicationControlTestContext() {
            clusterControl.activate();
        }
    }

    private static void registerBroker(int brokerId, ReplicationControlTestContext ctx) {
        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().
            setBrokerEpoch(brokerId + 100).setBrokerId(brokerId);
        brokerRecord.endPoints().add(new RegisterBrokerRecord.BrokerEndpoint().
            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
            setPort((short) 9092 + brokerId).
            setName("PLAINTEXT").
            setHost("localhost"));
        ctx.clusterControl.replay(brokerRecord);
    }

    private static void unfenceBroker(int brokerId,
                                      ReplicationControlTestContext ctx) throws Exception {
        ControllerResult<BrokerHeartbeatReply> result = ctx.replicationControl.
            processBrokerHeartbeat(new BrokerHeartbeatRequestData().
                setBrokerId(brokerId).setBrokerEpoch(brokerId + 100).setCurrentMetadataOffset(1).
                setWantFence(false).setWantShutDown(false), 0);
        assertEquals(new BrokerHeartbeatReply(true, false, false, false), result.response());
        ControllerTestUtils.replayAll(ctx.clusterControl, result.records());
    }

    @Test
    public void testCreateTopics() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
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

        registerBroker(0, ctx);
        unfenceBroker(0, ctx);
        registerBroker(1, ctx);
        unfenceBroker(1, ctx);
        registerBroker(2, ctx);
        unfenceBroker(2, ctx);
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
        Uuid fooId = result2.response().topics().find("foo").topicId();
        ControllerTestUtils.assertBatchIteratorContains(Arrays.asList(
            Arrays.asList(new ApiMessageAndVersion(new PartitionRecord().
                    setPartitionId(0).setTopicId(fooId).
                    setReplicas(Arrays.asList(2, 0, 1)).setIsr(Arrays.asList(2, 0, 1)).
                    setRemovingReplicas(null).setAddingReplicas(null).setLeader(2).
                    setLeaderEpoch(0).setPartitionEpoch(0), (short) 0),
                new ApiMessageAndVersion(new TopicRecord().
                    setTopicId(fooId).setName("foo"), (short) 0))),
            ctx.replicationControl.iterator(Long.MAX_VALUE));
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
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        for (int i = 0; i < 6; i++) {
            registerBroker(i, ctx);
            unfenceBroker(i, ctx);
        }
        CreatableTopicResult result = createTestTopic(replicationControl, "foo",
            new int[][] {
                new int[] {0, 1, 2},
                new int[] {1, 2, 3},
                new int[] {2, 3, 0},
                new int[] {0, 2, 1}
            });
        Set<TopicIdPartition> expectedPartitions = new HashSet<>();
        expectedPartitions.add(new TopicIdPartition(result.topicId(), 0));
        expectedPartitions.add(new TopicIdPartition(result.topicId(), 3));
        assertEquals(expectedPartitions, ControllerTestUtils.
            iteratorToSet(replicationControl.brokersToIsrs().iterator(0, true)));
        List<ApiMessageAndVersion> records = new ArrayList<>();
        replicationControl.handleNodeDeactivated(0, records);
        ControllerTestUtils.replayAll(replicationControl, records);
        assertEquals(Collections.emptySet(), ControllerTestUtils.
            iteratorToSet(replicationControl.brokersToIsrs().iterator(0, true)));
    }

    @Test
    public void testShrinkAndExpandIsr() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        for (int i = 0; i < 3; i++) {
            registerBroker(i, ctx);
            unfenceBroker(i, ctx);
        }
        CreatableTopicResult createTopicResult = createTestTopic(replicationControl, "foo",
            new int[][] {new int[] {0, 1, 2}});

        TopicIdPartition topicIdPartition = new TopicIdPartition(createTopicResult.topicId(), 0);
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        assertEquals(OptionalInt.of(0), currentLeader(replicationControl, topicIdPartition));
        long brokerEpoch = currentBrokerEpoch(ctx, 0);
        AlterIsrRequestData.PartitionData shrinkIsrRequest = newAlterIsrPartition(
            replicationControl, topicIdPartition, Arrays.asList(0, 1));
        ControllerResult<AlterIsrResponseData> shrinkIsrResult = sendAlterIsr(
            replicationControl, 0, brokerEpoch, "foo", shrinkIsrRequest);
        AlterIsrResponseData.PartitionData shrinkIsrResponse = assertAlterIsrResponse(
            shrinkIsrResult, topicPartition, NONE);
        assertConsistentAlterIsrResponse(replicationControl, topicIdPartition, shrinkIsrResponse);

        AlterIsrRequestData.PartitionData expandIsrRequest = newAlterIsrPartition(
            replicationControl, topicIdPartition, Arrays.asList(0, 1, 2));
        ControllerResult<AlterIsrResponseData> expandIsrResult = sendAlterIsr(
            replicationControl, 0, brokerEpoch, "foo", expandIsrRequest);
        AlterIsrResponseData.PartitionData expandIsrResponse = assertAlterIsrResponse(
            expandIsrResult, topicPartition, NONE);
        assertConsistentAlterIsrResponse(replicationControl, topicIdPartition, expandIsrResponse);
    }

    @Test
    public void testInvalidAlterIsrRequests() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        for (int i = 0; i < 3; i++) {
            registerBroker(i, ctx);
            unfenceBroker(i, ctx);
        }
        CreatableTopicResult createTopicResult = createTestTopic(replicationControl, "foo",
            new int[][] {new int[] {0, 1, 2}});

        TopicIdPartition topicIdPartition = new TopicIdPartition(createTopicResult.topicId(), 0);
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        assertEquals(OptionalInt.of(0), currentLeader(replicationControl, topicIdPartition));
        long brokerEpoch = currentBrokerEpoch(ctx, 0);

        // Invalid leader
        AlterIsrRequestData.PartitionData invalidLeaderRequest = newAlterIsrPartition(
            replicationControl, topicIdPartition, Arrays.asList(0, 1));
        ControllerResult<AlterIsrResponseData> invalidLeaderResult = sendAlterIsr(
            replicationControl, 1, currentBrokerEpoch(ctx, 1),
            "foo", invalidLeaderRequest);
        assertAlterIsrResponse(invalidLeaderResult, topicPartition, Errors.INVALID_REQUEST);

        // Stale broker epoch
        AlterIsrRequestData.PartitionData invalidBrokerEpochRequest = newAlterIsrPartition(
            replicationControl, topicIdPartition, Arrays.asList(0, 1));
        assertThrows(StaleBrokerEpochException.class, () -> sendAlterIsr(
            replicationControl, 0, brokerEpoch - 1, "foo", invalidBrokerEpochRequest));

        // Invalid leader epoch
        AlterIsrRequestData.PartitionData invalidLeaderEpochRequest = newAlterIsrPartition(
            replicationControl, topicIdPartition, Arrays.asList(0, 1));
        invalidLeaderEpochRequest.setLeaderEpoch(500);
        ControllerResult<AlterIsrResponseData> invalidLeaderEpochResult = sendAlterIsr(
            replicationControl, 1, currentBrokerEpoch(ctx, 1),
            "foo", invalidLeaderEpochRequest);
        assertAlterIsrResponse(invalidLeaderEpochResult, topicPartition, Errors.INVALID_REQUEST);

        // Invalid ISR (3 is not a valid replica)
        AlterIsrRequestData.PartitionData invalidIsrRequest1 = newAlterIsrPartition(
            replicationControl, topicIdPartition, Arrays.asList(0, 1));
        invalidIsrRequest1.setNewIsr(Arrays.asList(0, 1, 3));
        ControllerResult<AlterIsrResponseData> invalidIsrResult1 = sendAlterIsr(
            replicationControl, 1, currentBrokerEpoch(ctx, 1),
            "foo", invalidIsrRequest1);
        assertAlterIsrResponse(invalidIsrResult1, topicPartition, Errors.INVALID_REQUEST);

        // Invalid ISR (does not include leader 0)
        AlterIsrRequestData.PartitionData invalidIsrRequest2 = newAlterIsrPartition(
            replicationControl, topicIdPartition, Arrays.asList(0, 1));
        invalidIsrRequest2.setNewIsr(Arrays.asList(1, 2));
        ControllerResult<AlterIsrResponseData> invalidIsrResult2 = sendAlterIsr(
            replicationControl, 1, currentBrokerEpoch(ctx, 1),
            "foo", invalidIsrRequest2);
        assertAlterIsrResponse(invalidIsrResult2, topicPartition, Errors.INVALID_REQUEST);
    }

    private long currentBrokerEpoch(
        ReplicationControlTestContext ctx,
        int brokerId
    ) {
        Map<Integer, BrokerRegistration> registrations = ctx.clusterControl.brokerRegistrations();
        BrokerRegistration registration = registrations.get(brokerId);
        assertNotNull(registration, "No current registration for broker " + brokerId);
        return registration.epoch();
    }

    private OptionalInt currentLeader(
        ReplicationControlManager replicationControl,
        TopicIdPartition topicIdPartition
    ) {
        PartitionControlInfo partitionControl =
            replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        if (partitionControl.leader < 0) {
            return OptionalInt.empty();
        } else {
            return OptionalInt.of(partitionControl.leader);
        }
    }

    private AlterIsrRequestData.PartitionData newAlterIsrPartition(
        ReplicationControlManager replicationControl,
        TopicIdPartition topicIdPartition,
        List<Integer> newIsr
    ) {
        PartitionControlInfo partitionControl =
            replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        return new AlterIsrRequestData.PartitionData()
            .setPartitionIndex(0)
            .setLeaderEpoch(partitionControl.leaderEpoch)
            .setCurrentIsrVersion(partitionControl.partitionEpoch)
            .setNewIsr(newIsr);
    }

    private ControllerResult<AlterIsrResponseData> sendAlterIsr(
        ReplicationControlManager replicationControl,
        int brokerId,
        long brokerEpoch,
        String topic,
        AlterIsrRequestData.PartitionData partitionData
    ) throws Exception {
        AlterIsrRequestData request = new AlterIsrRequestData()
            .setBrokerId(brokerId)
            .setBrokerEpoch(brokerEpoch);

        AlterIsrRequestData.TopicData topicData = new AlterIsrRequestData.TopicData()
            .setName(topic);
        request.topics().add(topicData);
        topicData.partitions().add(partitionData);

        ControllerResult<AlterIsrResponseData> result = replicationControl.alterIsr(request);
        ControllerTestUtils.replayAll(replicationControl, result.records());
        return result;
    }

    private AlterIsrResponseData.PartitionData assertAlterIsrResponse(
        ControllerResult<AlterIsrResponseData> alterIsrResult,
        TopicPartition topicPartition,
        Errors expectedError
    ) {
        AlterIsrResponseData response = alterIsrResult.response();
        assertEquals(1, response.topics().size());

        AlterIsrResponseData.TopicData topicData = response.topics().get(0);
        assertEquals(topicPartition.topic(), topicData.name());
        assertEquals(1, topicData.partitions().size());

        AlterIsrResponseData.PartitionData partitionData = topicData.partitions().get(0);
        assertEquals(topicPartition.partition(), partitionData.partitionIndex());
        assertEquals(expectedError, Errors.forCode(partitionData.errorCode()));
        return partitionData;
    }

    private void assertConsistentAlterIsrResponse(
        ReplicationControlManager replicationControl,
        TopicIdPartition topicIdPartition,
        AlterIsrResponseData.PartitionData partitionData
    ) {
        PartitionControlInfo partitionControl =
            replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        assertEquals(partitionControl.leader, partitionData.leaderId());
        assertEquals(partitionControl.leaderEpoch, partitionData.leaderEpoch());
        assertEquals(partitionControl.partitionEpoch, partitionData.currentIsrVersion());
        List<Integer> expectedIsr = IntStream.of(partitionControl.isr).boxed().collect(Collectors.toList());
        assertEquals(expectedIsr, partitionData.isr());
    }

    private void assertCreatedTopicConfigs(
        ReplicationControlTestContext ctx,
        String topic,
        CreateTopicsRequestData.CreateableTopicConfigCollection requestConfigs
    ) {
        Map<String, String> configs = ctx.configurationControl.getConfigs(
            new ConfigResource(ConfigResource.Type.TOPIC, topic));
        assertEquals(requestConfigs.size(), configs.size());
        for (CreateTopicsRequestData.CreateableTopicConfig requestConfig : requestConfigs) {
            String value = configs.get(requestConfig.name());
            assertEquals(requestConfig.value(), value);
        }
    }

    private void assertEmptyTopicConfigs(
        ReplicationControlTestContext ctx,
        String topic
    ) {
        Map<String, String> configs = ctx.configurationControl.getConfigs(
            new ConfigResource(ConfigResource.Type.TOPIC, topic));
        assertEquals(Collections.emptyMap(), configs);
    }

    @Test
    public void testDeleteTopics() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        CreateTopicsRequestData.CreateableTopicConfigCollection requestConfigs =
            new CreateTopicsRequestData.CreateableTopicConfigCollection();
        requestConfigs.add(new CreateTopicsRequestData.CreateableTopicConfig().
            setName("cleanup.policy").setValue("compact"));
        requestConfigs.add(new CreateTopicsRequestData.CreateableTopicConfig().
            setName("min.cleanable.dirty.ratio").setValue("0.1"));
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(3).setReplicationFactor((short) 2).
            setConfigs(requestConfigs));
        registerBroker(0, ctx);
        unfenceBroker(0, ctx);
        registerBroker(1, ctx);
        unfenceBroker(1, ctx);
        ControllerResult<CreateTopicsResponseData> createResult =
            replicationControl.createTopics(request);
        CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
        Uuid topicId = createResult.response().topics().find("foo").topicId();
        expectedResponse.topics().add(new CreatableTopicResult().setName("foo").
            setNumPartitions(3).setReplicationFactor((short) 2).
            setErrorMessage(null).setErrorCode((short) 0).
            setTopicId(topicId));
        assertEquals(expectedResponse, createResult.response());
        // Until the records are replayed, no changes are made
        assertNull(replicationControl.getPartition(topicId, 0));
        assertEmptyTopicConfigs(ctx, "foo");
        ctx.replay(createResult.records());
        assertNotNull(replicationControl.getPartition(topicId, 0));
        assertNotNull(replicationControl.getPartition(topicId, 1));
        assertNotNull(replicationControl.getPartition(topicId, 2));
        assertNull(replicationControl.getPartition(topicId, 3));
        assertCreatedTopicConfigs(ctx, "foo", requestConfigs);

        assertEquals(Collections.singletonMap(topicId, new ResultOrError<>("foo")),
            replicationControl.findTopicNames(Long.MAX_VALUE, Collections.singleton(topicId)));
        assertEquals(Collections.singletonMap("foo", new ResultOrError<>(topicId)),
            replicationControl.findTopicIds(Long.MAX_VALUE, Collections.singleton("foo")));
        Uuid invalidId = new Uuid(topicId.getMostSignificantBits() + 1,
            topicId.getLeastSignificantBits());
        assertEquals(Collections.singletonMap(invalidId,
            new ResultOrError<>(new ApiError(UNKNOWN_TOPIC_ID))),
                replicationControl.findTopicNames(Long.MAX_VALUE, Collections.singleton(invalidId)));
        assertEquals(Collections.singletonMap("bar",
            new ResultOrError<>(new ApiError(UNKNOWN_TOPIC_OR_PARTITION))),
                replicationControl.findTopicIds(Long.MAX_VALUE, Collections.singleton("bar")));

        ControllerResult<Map<Uuid, ApiError>> invalidDeleteResult = replicationControl.
            deleteTopics(Collections.singletonList(invalidId));
        assertEquals(0, invalidDeleteResult.records().size());
        assertEquals(Collections.singletonMap(invalidId, new ApiError(UNKNOWN_TOPIC_ID, null)),
            invalidDeleteResult.response());
        ControllerResult<Map<Uuid, ApiError>> deleteResult = replicationControl.
            deleteTopics(Collections.singletonList(topicId));
        assertTrue(deleteResult.isAtomic());
        assertEquals(Collections.singletonMap(topicId, new ApiError(NONE, null)),
            deleteResult.response());
        assertEquals(1, deleteResult.records().size());
        ctx.replay(deleteResult.records());
        assertNull(replicationControl.getPartition(topicId, 0));
        assertNull(replicationControl.getPartition(topicId, 1));
        assertNull(replicationControl.getPartition(topicId, 2));
        assertNull(replicationControl.getPartition(topicId, 3));
        assertEquals(Collections.singletonMap(topicId, new ResultOrError<>(
            new ApiError(UNKNOWN_TOPIC_ID))), replicationControl.findTopicNames(
                Long.MAX_VALUE, Collections.singleton(topicId)));
        assertEquals(Collections.singletonMap("foo", new ResultOrError<>(
            new ApiError(UNKNOWN_TOPIC_OR_PARTITION))), replicationControl.findTopicIds(
                Long.MAX_VALUE, Collections.singleton("foo")));
        assertEmptyTopicConfigs(ctx, "foo");
    }
}
