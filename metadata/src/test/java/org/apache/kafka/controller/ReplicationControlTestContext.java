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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState;
import org.apache.kafka.common.message.AlterPartitionRequestData.PartitionData;
import org.apache.kafka.common.message.AlterPartitionRequestData.TopicData;
import org.apache.kafka.common.message.AlterPartitionResponseData;
import org.apache.kafka.common.message.AssignReplicasToDirsResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.metadata.AssignmentsHelper;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.apache.kafka.metadata.FakeKafkaConfigSchema;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.metadata.placement.StripedReplicaPlacer;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.server.policy.CreateTopicPolicy;
import org.apache.kafka.server.util.MockRandom;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
class ReplicationControlTestContext {
    private static final int BROKER_SESSION_TIMEOUT_MS = 1000;

    static class Builder {
        private Optional<CreateTopicPolicy> createTopicPolicy = Optional.empty();
        private MetadataVersion metadataVersion = MetadataVersion.latestTesting();
        private MockTime mockTime = new MockTime();
        private boolean isElrEnabled = false;
        private int maxRecordsPerBatch = KRaftConfigs.CONTROLLER_MAX_RECORDS_PER_BATCH_DEFAULT;
        private final Map<String, Object> staticConfig = new HashMap<>();

        Builder setCreateTopicPolicy(CreateTopicPolicy createTopicPolicy) {
            this.createTopicPolicy = Optional.of(createTopicPolicy);
            return this;
        }

        Builder setMetadataVersion(MetadataVersion metadataVersion) {
            this.metadataVersion = metadataVersion;
            return this;
        }

        Builder setIsElrEnabled(boolean isElrEnabled) {
            this.isElrEnabled = isElrEnabled;
            return this;
        }

        Builder setStaticConfig(String key, Object value) {
            this.staticConfig.put(key, value);
            return this;
        }

        Builder setMockTime(MockTime mockTime) {
            this.mockTime = mockTime;
            return this;
        }

        Builder setMaxRecordsPerBatch(int maxRecordsPerBatch) {
            this.maxRecordsPerBatch = maxRecordsPerBatch;
            return this;
        }

        ReplicationControlTestContext build() {
            return new ReplicationControlTestContext(metadataVersion,
                createTopicPolicy,
                mockTime,
                isElrEnabled,
                maxRecordsPerBatch,
                staticConfig);
        }
    }

    final SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
    final LogContext logContext = new LogContext();
    final MockTime time;
    final MockRandom random = new MockRandom();
    final FeatureControlManager featureControl;
    final ClusterControlManager clusterControl;
    final ConfigurationControlManager configurationControl;
    final ReplicationControlManager replicationControl;
    final OffsetControlManager offsetControlManager;

    void replay(List<ApiMessageAndVersion> records) {
        RecordTestUtils.replayAll(clusterControl, records);
        RecordTestUtils.replayAll(configurationControl, records);
        RecordTestUtils.replayAll(replicationControl, records);
    }

    private ReplicationControlTestContext(
        MetadataVersion metadataVersion,
        Optional<CreateTopicPolicy> createTopicPolicy,
        MockTime time,
        boolean isElrEnabled,
        int maxRecordsPerBatch,
        Map<String, Object> staticConfig
    ) {
        this.time = time;
        this.featureControl = new FeatureControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultSupportedFeatureMap(true),
                () -> Set.of(0))).
            setMaxRecordsPerBatch(KRaftConfigs.CONTROLLER_MAX_RECORDS_PER_BATCH_DEFAULT).
            build();
        this.featureControl.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(metadataVersion.featureLevel()));
        featureControl.replay(new FeatureLevelRecord()
            .setName(EligibleLeaderReplicasVersion.FEATURE_NAME)
                .setFeatureLevel(isElrEnabled ?
                    EligibleLeaderReplicasVersion.ELRV_1.featureLevel() :
                    EligibleLeaderReplicasVersion.ELRV_0.featureLevel())
        );
        this.clusterControl = new ClusterControlManager.Builder().
            setLogContext(logContext).
            setTime(time).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(TimeUnit.MILLISECONDS.convert(BROKER_SESSION_TIMEOUT_MS, TimeUnit.NANOSECONDS)).
            setReplicaPlacer(new StripedReplicaPlacer(random)).
            setFeatureControlManager(featureControl).
            setBrokerShutdownHandler(this::handleBrokerShutdown).
            build();
        this.configurationControl = new ConfigurationControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            setFeatureControl(featureControl).
            setStaticConfig(staticConfig).
            setKafkaConfigSchema(FakeKafkaConfigSchema.INSTANCE).
            setMaxRecordsPerBatch(KRaftConfigs.CONTROLLER_MAX_RECORDS_PER_BATCH_DEFAULT).
            build();
        this.offsetControlManager = new OffsetControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            build();
        this.replicationControl = new ReplicationControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            setLogContext(logContext).
            setMaxElectionsPerImbalance(Integer.MAX_VALUE).
            setMaxRecordsPerBatch(maxRecordsPerBatch).
            setConfigurationControl(configurationControl).
            setClusterControl(clusterControl).
            setCreateTopicPolicy(createTopicPolicy).
            setFeatureControl(featureControl).
            build();
        clusterControl.activate();
    }

    void handleBrokerShutdown(int brokerId, boolean isCleanShutdown, List<ApiMessageAndVersion> records) {
        replicationControl.handleBrokerShutdown(brokerId, isCleanShutdown, records);
    }

    CreatableTopicResult createTestTopic(String name,
                                         int numPartitions,
                                         short replicationFactor,
                                         short expectedErrorCode) {
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        CreatableTopic topic = new CreatableTopic().setName(name);
        topic.setNumPartitions(numPartitions).setReplicationFactor(replicationFactor);
        request.topics().add(topic);
        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> result =
            replicationControl.createTopics(requestContext, request, Set.of(name), false);
        CreatableTopicResult topicResult = result.response().topics().find(name);
        assertNotNull(topicResult);
        assertEquals(expectedErrorCode, topicResult.errorCode());
        if (expectedErrorCode == NONE.code()) {
            replay(result.records());
        }
        return topicResult;
    }

    CreatableTopicResult createTestTopic(String name, int[][] replicas) {
        return createTestTopic(name, replicas, Map.of(), (short) 0);
    }

    CreatableTopicResult createTestTopic(String name, int[][] replicas,
                                         short expectedErrorCode) {
        return createTestTopic(name, replicas, Map.of(), expectedErrorCode);
    }

    CreatableTopicResult createTestTopic(String name, int[][] replicas,
                                         Map<String, String> configs,
                                         short expectedErrorCode) {
        assertNotEquals(0, replicas.length);
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        CreatableTopic topic = new CreatableTopic().setName(name);
        topic.setNumPartitions(-1).setReplicationFactor((short) -1);
        for (int i = 0; i < replicas.length; i++) {
            topic.assignments().add(new CreatableReplicaAssignment().
                setPartitionIndex(i).setBrokerIds(Replicas.toList(replicas[i])));
        }
        configs.forEach((key, value) -> topic.configs().add(
                new CreateTopicsRequestData.CreatableTopicConfig()
                        .setName(key)
                        .setValue(value)
                )
        );
        request.topics().add(topic);
        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> result =
            replicationControl.createTopics(requestContext, request, Set.of(name), false);
        CreatableTopicResult topicResult = result.response().topics().find(name);
        assertNotNull(topicResult);
        assertEquals(expectedErrorCode, topicResult.errorCode());
        if (expectedErrorCode == NONE.code()) {
            assertEquals(replicas.length, topicResult.numPartitions());
            assertEquals(replicas[0].length, topicResult.replicationFactor());
            replay(result.records());
        }
        return topicResult;
    }

    void deleteTopic(ControllerRequestContext context, Uuid topicId) {
        ControllerResult<Map<Uuid, ApiError>> result = replicationControl.deleteTopics(context, Set.of(topicId));
        assertEquals(Set.of(topicId), result.response().keySet());
        assertEquals(NONE, result.response().get(topicId).error());
        assertEquals(1, result.records().size());

        ApiMessageAndVersion removeRecordAndVersion = result.records().get(0);
        assertInstanceOf(RemoveTopicRecord.class, removeRecordAndVersion.message());

        RemoveTopicRecord removeRecord = (RemoveTopicRecord) removeRecordAndVersion.message();
        assertEquals(topicId, removeRecord.topicId());

        replay(result.records());
    }

    void createPartitions(int count, String name, int[][] replicas, short expectedErrorCode) {
        assertNotEquals(0, replicas.length);
        CreatePartitionsTopic topic = new CreatePartitionsTopic().
            setName(name).
            setCount(count);
        for (int[] replica : replicas) {
            topic.assignments().add(new CreatePartitionsAssignment().
                setBrokerIds(Replicas.toList(replica)));
        }
        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);
        ControllerResult<List<CreatePartitionsTopicResult>> result =
            replicationControl.createPartitions(requestContext, List.of(topic));
        assertEquals(1, result.response().size());
        CreatePartitionsTopicResult topicResult = result.response().get(0);
        assertEquals(name, topicResult.name());
        assertEquals(expectedErrorCode, topicResult.errorCode());
        replay(result.records());
    }

    void registerBrokers(Integer... brokerIds) {
        Object[] brokersAndDirs = new Object[brokerIds.length * 2];
        for (int i = 0; i < brokerIds.length; i++) {
            brokersAndDirs[i * 2] = brokerIds[i];
            brokersAndDirs[i * 2 + 1] = List.of(
                Uuid.fromString("TESTBROKER" + Integer.toString(100000 + brokerIds[i]).substring(1) + "DIRAAAA")
            );
        }
        registerBrokersWithDirs(brokersAndDirs);
    }

    @SuppressWarnings("unchecked")
    void registerBrokersWithDirs(Object... brokerIdsAndDirs) {
        if (brokerIdsAndDirs.length % 2 != 0) {
            throw new IllegalArgumentException("uneven number of arguments");
        }
        for (int i = 0; i < brokerIdsAndDirs.length / 2; i++) {
            int brokerId = (int) brokerIdsAndDirs[i * 2];
            List<Uuid> logDirs = (List<Uuid>) brokerIdsAndDirs[i * 2 + 1];
            RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().
                setBrokerEpoch(defaultBrokerEpoch(brokerId)).setBrokerId(brokerId).
                    setRack(null).setLogDirs(logDirs);
            brokerRecord.endPoints().add(new RegisterBrokerRecord.BrokerEndpoint().
                setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
                setPort((short) 9092 + brokerId).
                setName("PLAINTEXT").
                setHost("localhost"));
            replay(List.of(new ApiMessageAndVersion(brokerRecord, (short) 3)));
        }
    }

    void handleBrokersShutdown(boolean isCleanShutdown, Integer... brokerIds) {
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (int brokerId : brokerIds) {
            replicationControl.handleBrokerShutdown(brokerId, isCleanShutdown, records);
        }
        replay(records);
    }

    void alterPartition(
        TopicIdPartition topicIdPartition,
        int leaderId,
        List<BrokerState> isrWithEpoch,
        LeaderRecoveryState leaderRecoveryState
    ) {
        BrokerRegistration registration = clusterControl.brokerRegistrations().get(leaderId);
        assertFalse(registration.fenced());

        PartitionRegistration partition = replicationControl.getPartition(
            topicIdPartition.topicId(),
            topicIdPartition.partitionId()
        );
        assertNotNull(partition);
        assertEquals(leaderId, partition.leader);

        PartitionData partitionData = new PartitionData()
            .setPartitionIndex(topicIdPartition.partitionId())
            .setPartitionEpoch(partition.partitionEpoch)
            .setLeaderEpoch(partition.leaderEpoch)
            .setLeaderRecoveryState(leaderRecoveryState.value())
            .setNewIsrWithEpochs(isrWithEpoch);

        TopicData topicData = new TopicData()
            .setTopicId(topicIdPartition.topicId())
            .setPartitions(List.of(partitionData));

        ControllerRequestContext requestContext =
            anonymousContextFor(ApiKeys.ALTER_PARTITION);
        ControllerResult<AlterPartitionResponseData> alterPartition = replicationControl.alterPartition(
            requestContext,
            new AlterPartitionRequestData()
                .setBrokerId(leaderId)
                .setBrokerEpoch(registration.epoch())
                .setTopics(List.of(topicData)));
        replay(alterPartition.records());
    }

    void unfenceBrokers(Integer... brokerIds) {
        for (int brokerId : brokerIds) {
            clusterControl.trackBrokerHeartbeat(brokerId, defaultBrokerEpoch(brokerId));
            ControllerResult<BrokerHeartbeatReply> result = replicationControl.
                processBrokerHeartbeat(new BrokerHeartbeatRequestData().
                    setBrokerId(brokerId).setBrokerEpoch(defaultBrokerEpoch(brokerId)).
                    setCurrentMetadataOffset(1).
                    setWantFence(false).setWantShutDown(false), 0);
            assertEquals(new BrokerHeartbeatReply(true, false, false, false),
                result.response());
            replay(result.records());
        }
    }

    void inControlledShutdownBrokers(Integer... brokerIds) {
        for (int brokerId : brokerIds) {
            BrokerRegistrationChangeRecord record = new BrokerRegistrationChangeRecord()
                .setBrokerId(brokerId)
                .setBrokerEpoch(defaultBrokerEpoch(brokerId))
                .setInControlledShutdown(BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.value());
            replay(List.of(new ApiMessageAndVersion(record, (short) 1)));
        }
    }

    void alterTopicConfig(
        String topic,
        String configKey,
        String configValue
    ) {
        ConfigRecord configRecord = new ConfigRecord()
            .setResourceType(ConfigResource.Type.TOPIC.id())
            .setResourceName(topic)
            .setName(configKey)
            .setValue(configValue);
        replay(List.of(new ApiMessageAndVersion(configRecord, (short) 0)));
    }

    void fenceBrokers(Integer... brokerIds) {
        fenceBrokers(Set.of(brokerIds));
    }

    void fenceBrokers(Set<Integer> brokerIds) {
        time.sleep(BROKER_SESSION_TIMEOUT_MS);

        Set<Integer> unfencedBrokerIds = clusterControl.brokerRegistrations().keySet().stream()
            .filter(brokerId -> !brokerIds.contains(brokerId))
            .collect(Collectors.toSet());
        unfenceBrokers(unfencedBrokerIds.toArray(new Integer[0]));

        ControllerResult<Boolean> fenceResult;
        do {
            fenceResult = replicationControl.maybeFenceOneStaleBroker();
            replay(fenceResult.records());
        } while (fenceResult.response().booleanValue());

        assertEquals(brokerIds, fencedBrokerIds());
    }

    long currentBrokerEpoch(int brokerId) {
        Map<Integer, BrokerRegistration> registrations = clusterControl.brokerRegistrations();
        BrokerRegistration registration = registrations.get(brokerId);
        assertNotNull(registration, "No current registration for broker " + brokerId);
        return registration.epoch();
    }

    OptionalInt currentLeader(TopicIdPartition topicIdPartition) {
        PartitionRegistration partition = replicationControl.
            getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        return (partition.leader < 0) ? OptionalInt.empty() : OptionalInt.of(partition.leader);
    }

    ControllerResult<AssignReplicasToDirsResponseData> assignReplicasToDirs(int brokerId, Map<TopicIdPartition, Uuid> assignment) {
        ControllerResult<AssignReplicasToDirsResponseData> result = replicationControl.handleAssignReplicasToDirs(
                AssignmentsHelper.buildRequestData(brokerId, defaultBrokerEpoch(brokerId), assignment));
        assertNotNull(result.response());
        assertEquals(NONE.code(), result.response().errorCode());
        replay(result.records());
        return result;
    }

    Set<Integer> fencedBrokerIds() {
        return clusterControl.brokerRegistrations().values()
                .stream()
                .filter(BrokerRegistration::fenced)
                .map(BrokerRegistration::id)
                .collect(Collectors.toSet());
    }

    void cordonBroker(int brokerId) {
        List<Uuid> dirs = clusterControl.brokerRegistrations().get(brokerId).directories();
        BrokerRegistrationChangeRecord record = new BrokerRegistrationChangeRecord()
            .setBrokerId(brokerId)
            .setBrokerEpoch(defaultBrokerEpoch(brokerId))
            .setCordonedLogDirs(dirs);
        clusterControl.replay(record);
    }


    static Long defaultBrokerEpoch(int brokerId) {
        return brokerId + 100L;
    }
}
