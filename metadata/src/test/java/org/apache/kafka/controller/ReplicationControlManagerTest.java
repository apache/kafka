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

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState;
import org.apache.kafka.common.message.AlterPartitionRequestData.PartitionData;
import org.apache.kafka.common.message.AlterPartitionRequestData.TopicData;
import org.apache.kafka.common.message.AlterPartitionResponseData;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirsResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersRequestData.TopicPartitions;
import org.apache.kafka.common.message.ElectLeadersRequestData.TopicPartitionsCollection;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingPartitionReassignment;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingTopicReassignment;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.controller.BrokerHeartbeatManager.BrokerHeartbeatState;
import org.apache.kafka.controller.ReplicationControlManager.KRaftClusterDescriber;
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
import org.apache.kafka.metadata.placement.UsableBroker;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.server.policy.CreateTopicPolicy;
import org.apache.kafka.server.util.MockRandom;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.config.TopicConfig.SEGMENT_BYTES_CONFIG;
import static org.apache.kafka.common.protocol.Errors.ELECTION_NOT_NEEDED;
import static org.apache.kafka.common.protocol.Errors.ELIGIBLE_LEADERS_NOT_AVAILABLE;
import static org.apache.kafka.common.protocol.Errors.FENCED_LEADER_EPOCH;
import static org.apache.kafka.common.protocol.Errors.INELIGIBLE_REPLICA;
import static org.apache.kafka.common.protocol.Errors.INVALID_PARTITIONS;
import static org.apache.kafka.common.protocol.Errors.INVALID_REPLICATION_FACTOR;
import static org.apache.kafka.common.protocol.Errors.INVALID_REPLICA_ASSIGNMENT;
import static org.apache.kafka.common.protocol.Errors.INVALID_TOPIC_EXCEPTION;
import static org.apache.kafka.common.protocol.Errors.NEW_LEADER_ELECTED;
import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.common.protocol.Errors.NOT_CONTROLLER;
import static org.apache.kafka.common.protocol.Errors.NOT_LEADER_OR_FOLLOWER;
import static org.apache.kafka.common.protocol.Errors.NO_REASSIGNMENT_IN_PROGRESS;
import static org.apache.kafka.common.protocol.Errors.OPERATION_NOT_ATTEMPTED;
import static org.apache.kafka.common.protocol.Errors.POLICY_VIOLATION;
import static org.apache.kafka.common.protocol.Errors.PREFERRED_LEADER_NOT_AVAILABLE;
import static org.apache.kafka.common.protocol.Errors.THROTTLING_QUOTA_EXCEEDED;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_ID;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION;
import static org.apache.kafka.controller.ControllerRequestContextUtil.QUOTA_EXCEEDED_IN_TEST_MSG;
import static org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextFor;
import static org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextWithMutationQuotaExceededFor;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.apache.kafka.metadata.placement.PartitionAssignmentTest.partitionAssignment;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class ReplicationControlManagerTest {
    private static final Logger log = LoggerFactory.getLogger(ReplicationControlManagerTest.class);
    private static final int BROKER_SESSION_TIMEOUT_MS = 1000;

    private static class ReplicationControlTestContext {
        private static class Builder {
            private Optional<CreateTopicPolicy> createTopicPolicy = Optional.empty();
            private MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            private MockTime mockTime = new MockTime();
            private boolean isElrEnabled = false;
            private final Map<String, Object> staticConfig = new HashMap<>();

            Builder setCreateTopicPolicy(CreateTopicPolicy createTopicPolicy) {
                this.createTopicPolicy = Optional.of(createTopicPolicy);
                return this;
            }

            Builder setMetadataVersion(MetadataVersion metadataVersion) {
                this.metadataVersion = metadataVersion;
                return this;
            }

            Builder setIsElrEnabled(Boolean isElrEnabled) {
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

            ReplicationControlTestContext build() {
                return new ReplicationControlTestContext(metadataVersion,
                    createTopicPolicy,
                    mockTime,
                    isElrEnabled,
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
            Map<String, Object> staticConfig
        ) {
            this.time = time;
            this.featureControl = new FeatureControlManager.Builder().
                setSnapshotRegistry(snapshotRegistry).
                setQuorumFeatures(new QuorumFeatures(0,
                    QuorumFeatures.defaultSupportedFeatureMap(true),
                    Collections.singletonList(0))).
                setMetadataVersion(metadataVersion).
                build();
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
                setBrokerUncleanShutdownHandler(this::handleUncleanBrokerShutdown).
                build();
            this.configurationControl = new ConfigurationControlManager.Builder().
                setSnapshotRegistry(snapshotRegistry).
                setFeatureControl(featureControl).
                setStaticConfig(staticConfig).
                setKafkaConfigSchema(FakeKafkaConfigSchema.INSTANCE).
                build();
            this.offsetControlManager = new OffsetControlManager.Builder().
                setSnapshotRegistry(snapshotRegistry).
                build();
            this.replicationControl = new ReplicationControlManager.Builder().
                setSnapshotRegistry(snapshotRegistry).
                setLogContext(logContext).
                setMaxElectionsPerImbalance(Integer.MAX_VALUE).
                setConfigurationControl(configurationControl).
                setClusterControl(clusterControl).
                setCreateTopicPolicy(createTopicPolicy).
                setFeatureControl(featureControl).
                build();
            clusterControl.activate();
        }

        void handleUncleanBrokerShutdown(int brokerId, List<ApiMessageAndVersion> records) {
            replicationControl.handleBrokerUncleanShutdown(brokerId, records);
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
                replicationControl.createTopics(requestContext, request, Collections.singleton(name));
            CreatableTopicResult topicResult = result.response().topics().find(name);
            assertNotNull(topicResult);
            assertEquals(expectedErrorCode, topicResult.errorCode());
            if (expectedErrorCode == NONE.code()) {
                replay(result.records());
            }
            return topicResult;
        }

        CreatableTopicResult createTestTopic(String name, int[][] replicas) {
            return createTestTopic(name, replicas, Collections.emptyMap(), (short) 0);
        }

        CreatableTopicResult createTestTopic(String name, int[][] replicas,
                                             short expectedErrorCode) {
            return createTestTopic(name, replicas, Collections.emptyMap(), expectedErrorCode);
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
                replicationControl.createTopics(requestContext, request, Collections.singleton(name));
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
            ControllerResult<Map<Uuid, ApiError>> result = replicationControl.deleteTopics(context, Collections.singleton(topicId));
            assertEquals(Collections.singleton(topicId), result.response().keySet());
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
                replicationControl.createPartitions(requestContext, Collections.singletonList(topic));
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
                brokersAndDirs[i * 2 + 1] = Collections.singletonList(
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
                replay(Collections.singletonList(new ApiMessageAndVersion(brokerRecord, (short) 3)));
            }
        }

        void handleBrokersUncleanShutdown(Integer... brokerIds) {
            List<ApiMessageAndVersion> records = new ArrayList<>();
            for (int brokerId : brokerIds) {
                replicationControl.handleBrokerUncleanShutdown(brokerId, records);
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

            String topicName = replicationControl.getTopic(topicIdPartition.topicId()).name();
            TopicData topicData = new TopicData()
                .setTopicName(topicName)
                .setTopicId(topicIdPartition.topicId())
                .setPartitions(singletonList(partitionData));

            ControllerRequestContext requestContext =
                anonymousContextFor(ApiKeys.ALTER_PARTITION);
            ControllerResult<AlterPartitionResponseData> alterPartition = replicationControl.alterPartition(
                requestContext,
                new AlterPartitionRequestData()
                    .setBrokerId(leaderId)
                    .setBrokerEpoch(registration.epoch())
                    .setTopics(singletonList(topicData)));
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
                replay(singletonList(new ApiMessageAndVersion(record, (short) 1)));
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
            replay(singletonList(new ApiMessageAndVersion(configRecord, (short) 0)));
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

    }

    static CreateTopicsResponseData withoutConfigs(CreateTopicsResponseData data) {
        data.topics().forEach(t -> t.configs().clear());
        return data;
    }

    private static class MockCreateTopicPolicy implements CreateTopicPolicy {
        private final List<RequestMetadata> expecteds;
        private final AtomicLong index = new AtomicLong(0);

        MockCreateTopicPolicy(List<RequestMetadata> expecteds) {
            this.expecteds = expecteds;
        }

        @Override
        public void validate(RequestMetadata actual) throws PolicyViolationException {
            long curIndex = index.getAndIncrement();
            if (curIndex >= expecteds.size()) {
                throw new PolicyViolationException("Unexpected topic creation: index " +
                    "out of range at " + curIndex);
            }
            RequestMetadata expected = expecteds.get((int) curIndex);
            if (!expected.equals(actual)) {
                throw new PolicyViolationException("Expected: " + expected +
                    ". Got: " + actual);
            }
        }

        @Override
        public void close() {
            // nothing to do
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // nothing to do
        }
    }

    @Test
    public void testExcessiveNumberOfTopicsCannotBeCreated() {
        // number of partitions is explicitly set without assignments
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
                setNumPartitions(5000).setReplicationFactor((short) 1));
        request.topics().add(new CreatableTopic().setName("bar").
                setNumPartitions(5000).setReplicationFactor((short) 1));
        request.topics().add(new CreatableTopic().setName("baz").
                setNumPartitions(1).setReplicationFactor((short) 1));
        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
        PolicyViolationException error = assertThrows(
                PolicyViolationException.class,
                () -> replicationControl.createTopics(requestContext, request, Set.of("foo", "bar", "baz")));
        assertEquals(error.getMessage(), "Excessively large number of partitions per request.");
    }

    @Test
    public void testExcessiveNumberOfTopicsCannotBeCreatedWithAssignments() {
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
                setNumPartitions(-1).setReplicationFactor((short) 1));
        CreateTopicsRequestData.CreatableReplicaAssignmentCollection assignments =
                new CreateTopicsRequestData.CreatableReplicaAssignmentCollection();
        assignments.add(new CreatableReplicaAssignment().setPartitionIndex(1));
        assignments.add(new CreatableReplicaAssignment().setPartitionIndex(2));
        request.topics().add(new CreatableTopic()
                .setName("baz")
                .setAssignments(assignments));
        PolicyViolationException error = assertThrows(
                PolicyViolationException.class,
                () -> ReplicationControlManager.validateTotalNumberOfPartitions(request, 9999)
        );
        assertEquals(error.getMessage(), "Excessively large number of partitions per request.");
    }

    @Test
    public void testCreateTopics() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(-1).setReplicationFactor((short) -1));

        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> result =
            replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));
        CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
        expectedResponse.topics().add(new CreatableTopicResult().setName("foo").
            setErrorCode(INVALID_REPLICATION_FACTOR.code()).
                setErrorMessage("Unable to replicate the partition 3 time(s): All " +
                    "brokers are currently fenced."));
        assertEquals(expectedResponse, result.response());

        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0);
        ctx.inControlledShutdownBrokers(0);

        ControllerResult<CreateTopicsResponseData> result2 =
            replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));
        CreateTopicsResponseData expectedResponse2 = new CreateTopicsResponseData();
        expectedResponse2.topics().add(new CreatableTopicResult().setName("foo").
            setErrorCode(INVALID_REPLICATION_FACTOR.code()).
            setErrorMessage("Unable to replicate the partition 3 time(s): All " +
                "brokers are currently fenced or in controlled shutdown."));
        assertEquals(expectedResponse2, result2.response());

        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);

        ControllerResult<CreateTopicsResponseData> result3 =
            replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));
        CreateTopicsResponseData expectedResponse3 = new CreateTopicsResponseData();
        expectedResponse3.topics().add(new CreatableTopicResult().setName("foo").
            setNumPartitions(1).setReplicationFactor((short) 3).
            setErrorMessage(null).setErrorCode((short) 0).
            setTopicId(result3.response().topics().find("foo").topicId()));
        assertEquals(expectedResponse3, withoutConfigs(result3.response()));
        ctx.replay(result3.records());
        assertEquals(new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 0}).
            setDirectories(new Uuid[] {
                    Uuid.fromString("TESTBROKER00001DIRAAAA"),
                    Uuid.fromString("TESTBROKER00002DIRAAAA"),
                    Uuid.fromString("TESTBROKER00000DIRAAAA")
            }).
            setIsr(new int[] {1, 2, 0}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(0).setPartitionEpoch(0).build(),
            replicationControl.getPartition(
                ((TopicRecord) result3.records().get(0).message()).topicId(), 0));
        ControllerResult<CreateTopicsResponseData> result4 =
                replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));
        CreateTopicsResponseData expectedResponse4 = new CreateTopicsResponseData();
        expectedResponse4.topics().add(new CreatableTopicResult().setName("foo").
                setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code()).
                setErrorMessage("Topic 'foo' already exists."));
        assertEquals(expectedResponse4, result4.response());
    }

    @Test
    public void testCreateTopicsWithMutationQuotaExceeded() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(-1).setReplicationFactor((short) -1));
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        ControllerRequestContext requestContext =
            anonymousContextWithMutationQuotaExceededFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> result =
            replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));
        CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
        expectedResponse.topics().add(new CreatableTopicResult().setName("foo").
            setErrorCode(THROTTLING_QUOTA_EXCEEDED.code()).
            setErrorMessage(QUOTA_EXCEEDED_IN_TEST_MSG));
        assertEquals(expectedResponse, result.response());
    }

    @Test
    public void testCreateTopicsISRInvariants() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;

        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(-1).setReplicationFactor((short) -1));

        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1);
        ctx.inControlledShutdownBrokers(1);

        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> result =
            replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));

        CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
        expectedResponse.topics().add(new CreatableTopicResult().setName("foo").
            setNumPartitions(1).setReplicationFactor((short) 3).
            setErrorMessage(null).setErrorCode((short) 0).
            setTopicId(result.response().topics().find("foo").topicId()));
        for (CreatableTopicResult topic : result.response().topics()) {
            topic.configs().clear();
        }
        assertEquals(expectedResponse, result.response());

        ctx.replay(result.records());

        // Broker 2 cannot be in the ISR because it is fenced and broker 1
        // cannot be in the ISR because it is in controlled shutdown.
        assertEquals(
            new PartitionRegistration.Builder().setReplicas(new int[]{1, 0, 2}).
                setDirectories(new Uuid[] {
                        Uuid.fromString("TESTBROKER00001DIRAAAA"),
                        Uuid.fromString("TESTBROKER00000DIRAAAA"),
                        Uuid.fromString("TESTBROKER00002DIRAAAA")
                }).
                setIsr(new int[]{0}).
                setLeader(0).
                setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                setLeaderEpoch(0).
                setPartitionEpoch(0).build(),
            replicationControl.getPartition(
                ((TopicRecord) result.records().get(0).message()).topicId(), 0));
    }

    @Test
    public void testCreateTopicsWithConfigs() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);

        CreateTopicsRequestData.CreatableTopicConfigCollection validConfigs =
            new CreateTopicsRequestData.CreatableTopicConfigCollection();
        validConfigs.add(
            new CreateTopicsRequestData.CreatableTopicConfig()
                .setName("foo")
                .setValue("notNull")
        );
        CreateTopicsRequestData request1 = new CreateTopicsRequestData();
        request1.topics().add(new CreatableTopic().setName("foo")
            .setNumPartitions(-1).setReplicationFactor((short) -1)
            .setConfigs(validConfigs));

        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> result1 =
            replicationControl.createTopics(requestContext, request1, Collections.singleton("foo"));
        assertEquals((short) 0, result1.response().topics().find("foo").errorCode());

        List<ApiMessageAndVersion> records1 = result1.records();
        assertEquals(3, records1.size());
        ApiMessageAndVersion record0 = records1.get(0);
        assertEquals(TopicRecord.class, record0.message().getClass());

        ApiMessageAndVersion record1 = records1.get(1);
        assertEquals(ConfigRecord.class, record1.message().getClass());

        ApiMessageAndVersion lastRecord = records1.get(2);
        assertEquals(PartitionRecord.class, lastRecord.message().getClass());

        ctx.replay(result1.records());
        assertEquals(
            "notNull",
            ctx.configurationControl.getConfigs(new ConfigResource(ConfigResource.Type.TOPIC, "foo")).get("foo")
        );

        CreateTopicsRequestData.CreatableTopicConfigCollection invalidConfigs =
            new CreateTopicsRequestData.CreatableTopicConfigCollection();
        invalidConfigs.add(
            new CreateTopicsRequestData.CreatableTopicConfig()
                .setName("foo")
                .setValue(null)
        );
        CreateTopicsRequestData request2 = new CreateTopicsRequestData();
        request2.topics().add(new CreatableTopic().setName("bar")
            .setNumPartitions(-1).setReplicationFactor((short) -1)
            .setConfigs(invalidConfigs));

        ControllerResult<CreateTopicsResponseData> result2 =
            replicationControl.createTopics(requestContext, request2, Collections.singleton("bar"));
        assertEquals(Errors.INVALID_CONFIG.code(), result2.response().topics().find("bar").errorCode());
        assertEquals(
            "Null value not supported for topic configs: foo",
            result2.response().topics().find("bar").errorMessage()
        );

        CreateTopicsRequestData request3 = new CreateTopicsRequestData();
        request3.topics().add(new CreatableTopic().setName("baz")
            .setNumPartitions(-1).setReplicationFactor((short) -2)
            .setConfigs(validConfigs));

        ControllerResult<CreateTopicsResponseData> result3 =
            replicationControl.createTopics(requestContext, request3, Collections.singleton("baz"));
        assertEquals(INVALID_REPLICATION_FACTOR.code(), result3.response().topics().find("baz").errorCode());
        assertEquals(Collections.emptyList(), result3.records());

        // Test request with multiple topics together.
        CreateTopicsRequestData request4 = new CreateTopicsRequestData();
        String batchedTopic1 = "batched-topic-1";
        request4.topics().add(new CreatableTopic().setName(batchedTopic1)
            .setNumPartitions(-1).setReplicationFactor((short) -1)
            .setConfigs(validConfigs));
        String batchedTopic2 = "batched-topic2";
        request4.topics().add(new CreatableTopic().setName(batchedTopic2)
            .setNumPartitions(-1).setReplicationFactor((short) -2)
            .setConfigs(validConfigs));

        Set<String> request4Topics = new HashSet<>();
        request4Topics.add(batchedTopic1);
        request4Topics.add(batchedTopic2);
        ControllerResult<CreateTopicsResponseData> result4 =
            replicationControl.createTopics(requestContext, request4, request4Topics);

        assertEquals(Errors.NONE.code(), result4.response().topics().find(batchedTopic1).errorCode());
        assertEquals(INVALID_REPLICATION_FACTOR.code(), result4.response().topics().find(batchedTopic2).errorCode());

        assertEquals(3, result4.records().size());
        assertEquals(TopicRecord.class, result4.records().get(0).message().getClass());
        TopicRecord batchedTopic1Record = (TopicRecord) result4.records().get(0).message();
        assertEquals(batchedTopic1, batchedTopic1Record.name());
        assertEquals(new ConfigRecord()
            .setResourceName(batchedTopic1)
            .setResourceType(ConfigResource.Type.TOPIC.id())
            .setName("foo")
            .setValue("notNull"),
            result4.records().get(1).message());
        assertEquals(PartitionRecord.class, result4.records().get(2).message().getClass());
        assertEquals(batchedTopic1Record.topicId(), ((PartitionRecord) result4.records().get(2).message()).topicId());
    }

    @ParameterizedTest(name = "testCreateTopicsWithValidateOnlyFlag with mutationQuotaExceeded: {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateTopicsWithValidateOnlyFlag(boolean mutationQuotaExceeded) {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        CreateTopicsRequestData request = new CreateTopicsRequestData().setValidateOnly(true);
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(1).setReplicationFactor((short) 3));
        ControllerRequestContext requestContext = mutationQuotaExceeded ?
            anonymousContextWithMutationQuotaExceededFor(ApiKeys.CREATE_TOPICS) :
            anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> result =
            ctx.replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));
        assertEquals(0, result.records().size());
        CreatableTopicResult topicResult = result.response().topics().find("foo");
        if (mutationQuotaExceeded) {
            assertEquals(THROTTLING_QUOTA_EXCEEDED.code(), topicResult.errorCode());
        } else {
            assertEquals(NONE.code(), topicResult.errorCode());
        }
    }

    @Test
    public void testInvalidCreateTopicsWithValidateOnlyFlag() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        CreateTopicsRequestData request = new CreateTopicsRequestData().setValidateOnly(true);
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(1).setReplicationFactor((short) 4));
        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> result =
            ctx.replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));
        assertEquals(0, result.records().size());
        CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
        expectedResponse.topics().add(new CreatableTopicResult().setName("foo").
            setErrorCode(INVALID_REPLICATION_FACTOR.code()).
            setErrorMessage("Unable to replicate the partition 4 time(s): The target " +
                "replication factor of 4 cannot be reached because only 3 broker(s) " +
                "are registered."));
        assertEquals(expectedResponse, result.response());
    }

    @Test
    public void testCreateTopicsWithPolicy() {
        MockCreateTopicPolicy createTopicPolicy = new MockCreateTopicPolicy(asList(
            new CreateTopicPolicy.RequestMetadata("foo", 2, (short) 2,
                null, Collections.emptyMap()),
            new CreateTopicPolicy.RequestMetadata("bar", 3, (short) 2,
                null, Collections.emptyMap()),
            new CreateTopicPolicy.RequestMetadata("baz", null, null,
                Collections.singletonMap(0, asList(2, 1, 0)),
                Collections.singletonMap(SEGMENT_BYTES_CONFIG, "12300000")),
            new CreateTopicPolicy.RequestMetadata("quux", null, null,
                Collections.singletonMap(0, asList(2, 1, 0)), Collections.emptyMap())));
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().
                setCreateTopicPolicy(createTopicPolicy).
                build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        ctx.createTestTopic("foo", 2, (short) 2, NONE.code());
        ctx.createTestTopic("bar", 3, (short) 3, POLICY_VIOLATION.code());
        ctx.createTestTopic("baz", new int[][] {new int[] {2, 1, 0}},
            Collections.singletonMap(SEGMENT_BYTES_CONFIG, "12300000"), NONE.code());
        ctx.createTestTopic("quux", new int[][] {new int[] {1, 2, 0}}, POLICY_VIOLATION.code());
    }

    @Test
    public void testCreateTopicWithCollisionChars() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);

        CreatableTopicResult initialTopic = ctx.createTestTopic("foo.bar", 2, (short) 2, NONE.code());
        assertEquals(2, ctx.replicationControl.getTopic(initialTopic.topicId()).numPartitions(Long.MAX_VALUE));
        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.DELETE_TOPICS);
        ctx.deleteTopic(requestContext, initialTopic.topicId());

        CreatableTopicResult recreatedTopic = ctx.createTestTopic("foo.bar", 4, (short) 2, NONE.code());
        assertNotEquals(initialTopic.topicId(), recreatedTopic.topicId());
        assertEquals(4, ctx.replicationControl.getTopic(recreatedTopic.topicId()).numPartitions(Long.MAX_VALUE));
    }

    @Test
    public void testValidateNewTopicNames() {
        Map<String, ApiError> topicErrors = new HashMap<>();
        CreatableTopicCollection topics = new CreatableTopicCollection();
        topics.add(new CreatableTopic().setName(""));
        topics.add(new CreatableTopic().setName("woo"));
        topics.add(new CreatableTopic().setName("."));
        ReplicationControlManager.validateNewTopicNames(topicErrors, topics, Collections.emptyMap());
        Map<String, ApiError> expectedTopicErrors = new HashMap<>();
        expectedTopicErrors.put("", new ApiError(INVALID_TOPIC_EXCEPTION,
            "Topic name is invalid: the empty string is not allowed"));
        expectedTopicErrors.put(".", new ApiError(INVALID_TOPIC_EXCEPTION,
            "Topic name is invalid: '.' is not allowed"));
        assertEquals(expectedTopicErrors, topicErrors);
    }

    @Test
    public void testTopicNameCollision() {
        Map<String, ApiError> topicErrors = new HashMap<>();
        CreatableTopicCollection topics = new CreatableTopicCollection();
        topics.add(new CreatableTopic().setName("foo.bar"));
        topics.add(new CreatableTopic().setName("woo.bar_foo"));
        Map<String, Set<String>> collisionMap = new HashMap<>();
        collisionMap.put("foo_bar", new TreeSet<>(singletonList("foo_bar")));
        collisionMap.put("woo_bar_foo", new TreeSet<>(Arrays.asList("woo.bar.foo", "woo_bar.foo")));
        ReplicationControlManager.validateNewTopicNames(topicErrors, topics, collisionMap);
        Map<String, ApiError> expectedTopicErrors = new HashMap<>();
        expectedTopicErrors.put("foo.bar", new ApiError(INVALID_TOPIC_EXCEPTION,
            "Topic 'foo.bar' collides with existing topic: foo_bar"));
        expectedTopicErrors.put("woo.bar_foo", new ApiError(INVALID_TOPIC_EXCEPTION,
            "Topic 'woo.bar_foo' collides with existing topic: woo.bar.foo"));
        assertEquals(expectedTopicErrors, topicErrors);
    }

    @Test
    public void testRemoveLeaderships() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3);
        ctx.unfenceBrokers(0, 1, 2, 3);
        CreatableTopicResult result = ctx.createTestTopic("foo",
            new int[][] {
                new int[] {0, 1, 2},
                new int[] {1, 2, 3},
                new int[] {2, 3, 0},
                new int[] {0, 2, 1}
            });
        Set<TopicIdPartition> expectedPartitions = new HashSet<>();
        expectedPartitions.add(new TopicIdPartition(result.topicId(), 0));
        expectedPartitions.add(new TopicIdPartition(result.topicId(), 3));
        assertEquals(expectedPartitions, RecordTestUtils.
            iteratorToSet(replicationControl.brokersToIsrs().iterator(0, true)));
        List<ApiMessageAndVersion> records = new ArrayList<>();
        replicationControl.handleBrokerFenced(0, records);
        ctx.replay(records);
        assertEquals(Collections.emptySet(), RecordTestUtils.
            iteratorToSet(replicationControl.brokersToIsrs().iterator(0, true)));
    }

    @Test
    public void testShrinkAndExpandIsr() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
            new int[][] {new int[] {0, 1, 2}});

        TopicIdPartition topicIdPartition = new TopicIdPartition(createTopicResult.topicId(), 0);
        assertEquals(OptionalInt.of(0), ctx.currentLeader(topicIdPartition));
        long brokerEpoch = ctx.currentBrokerEpoch(0);
        PartitionData shrinkIsrRequest = newAlterPartition(
            replicationControl, topicIdPartition, isrWithDefaultEpoch(0, 1), LeaderRecoveryState.RECOVERED);
        ControllerResult<AlterPartitionResponseData> shrinkIsrResult = sendAlterPartition(
            replicationControl, 0, brokerEpoch, topicIdPartition.topicId(), shrinkIsrRequest);
        AlterPartitionResponseData.PartitionData shrinkIsrResponse = assertAlterPartitionResponse(
            shrinkIsrResult, topicIdPartition, NONE);
        assertConsistentAlterPartitionResponse(replicationControl, topicIdPartition, shrinkIsrResponse);

        PartitionData expandIsrRequest = newAlterPartition(
            replicationControl, topicIdPartition, isrWithDefaultEpoch(0, 1, 2), LeaderRecoveryState.RECOVERED);
        ControllerResult<AlterPartitionResponseData> expandIsrResult = sendAlterPartition(
            replicationControl, 0, brokerEpoch, topicIdPartition.topicId(), expandIsrRequest);
        AlterPartitionResponseData.PartitionData expandIsrResponse = assertAlterPartitionResponse(
            expandIsrResult, topicIdPartition, NONE);
        assertConsistentAlterPartitionResponse(replicationControl, topicIdPartition, expandIsrResponse);
    }

    @Test
    public void testEligibleLeaderReplicas_ShrinkAndExpandIsr() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().setIsElrEnabled(true).build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
            new int[][] {new int[] {0, 1, 2}});

        TopicIdPartition topicIdPartition = new TopicIdPartition(createTopicResult.topicId(), 0);
        assertEquals(OptionalInt.of(0), ctx.currentLeader(topicIdPartition));
        long brokerEpoch = ctx.currentBrokerEpoch(0);
        ctx.alterTopicConfig("foo", TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");

        // Change ISR to {0}.
        PartitionData shrinkIsrRequest = newAlterPartition(
            replicationControl, topicIdPartition, isrWithDefaultEpoch(0), LeaderRecoveryState.RECOVERED);

        ControllerResult<AlterPartitionResponseData> shrinkIsrResult = sendAlterPartition(
            replicationControl, 0, brokerEpoch, topicIdPartition.topicId(), shrinkIsrRequest);
        AlterPartitionResponseData.PartitionData shrinkIsrResponse = assertAlterPartitionResponse(
            shrinkIsrResult, topicIdPartition, NONE);
        assertConsistentAlterPartitionResponse(replicationControl, topicIdPartition, shrinkIsrResponse);
        PartitionRegistration partition = replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        assertArrayEquals(new int[]{1, 2}, partition.elr, partition.toString());
        assertArrayEquals(new int[]{}, partition.lastKnownElr, partition.toString());

        PartitionData expandIsrRequest = newAlterPartition(
            replicationControl, topicIdPartition, isrWithDefaultEpoch(0, 1), LeaderRecoveryState.RECOVERED);
        ControllerResult<AlterPartitionResponseData> expandIsrResult = sendAlterPartition(
            replicationControl, 0, brokerEpoch, topicIdPartition.topicId(), expandIsrRequest);
        AlterPartitionResponseData.PartitionData expandIsrResponse = assertAlterPartitionResponse(
            expandIsrResult, topicIdPartition, NONE);
        assertConsistentAlterPartitionResponse(replicationControl, topicIdPartition, expandIsrResponse);
        partition = replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        assertArrayEquals(new int[]{}, partition.elr, partition.toString());
        assertArrayEquals(new int[]{}, partition.lastKnownElr, partition.toString());
    }

    @Test
    public void testEligibleLeaderReplicas_BrokerFence() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().setIsElrEnabled(true).build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3);
        ctx.unfenceBrokers(0, 1, 2, 3);
        CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
            new int[][] {new int[] {0, 1, 2, 3}});

        TopicIdPartition topicIdPartition = new TopicIdPartition(createTopicResult.topicId(), 0);
        assertEquals(OptionalInt.of(0), ctx.currentLeader(topicIdPartition));
        ctx.alterTopicConfig("foo", TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3");

        ctx.fenceBrokers(Set.of(2, 3));

        PartitionRegistration partition = replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        assertArrayEquals(new int[]{3}, partition.elr, partition.toString());
        assertArrayEquals(new int[]{}, partition.lastKnownElr, partition.toString());

        ctx.fenceBrokers(Set.of(1, 2, 3));

        partition = replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        assertArrayEquals(new int[]{1, 3}, partition.elr, partition.toString());
        assertArrayEquals(new int[]{}, partition.lastKnownElr, partition.toString());

        ctx.unfenceBrokers(0, 1, 2, 3);
        partition = replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        assertArrayEquals(new int[]{1, 3}, partition.elr, partition.toString());
        assertArrayEquals(new int[]{}, partition.lastKnownElr, partition.toString());
    }

    @Test
    public void testEligibleLeaderReplicas_DeleteTopic() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().setIsElrEnabled(true).build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
            new int[][] {new int[] {0, 1, 2}});

        TopicIdPartition topicIdPartition = new TopicIdPartition(createTopicResult.topicId(), 0);
        assertEquals(OptionalInt.of(0), ctx.currentLeader(topicIdPartition));
        long brokerEpoch = ctx.currentBrokerEpoch(0);
        ctx.alterTopicConfig("foo", TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");

        // Change ISR to {0}.
        PartitionData shrinkIsrRequest = newAlterPartition(
            replicationControl, topicIdPartition, isrWithDefaultEpoch(0), LeaderRecoveryState.RECOVERED);

        ControllerResult<AlterPartitionResponseData> shrinkIsrResult = sendAlterPartition(
            replicationControl, 0, brokerEpoch, topicIdPartition.topicId(), shrinkIsrRequest);
        AlterPartitionResponseData.PartitionData shrinkIsrResponse = assertAlterPartitionResponse(
            shrinkIsrResult, topicIdPartition, NONE);
        assertConsistentAlterPartitionResponse(replicationControl, topicIdPartition, shrinkIsrResponse);
        PartitionRegistration partition = replicationControl.getPartition(topicIdPartition.topicId(),
            topicIdPartition.partitionId());
        assertArrayEquals(new int[]{1, 2}, partition.elr, partition.toString());
        assertArrayEquals(new int[]{}, partition.lastKnownElr, partition.toString());
        assertTrue(replicationControl.brokersToElrs().partitionsWithBrokerInElr(1).hasNext());

        ControllerRequestContext deleteTopicsRequestContext = anonymousContextFor(ApiKeys.DELETE_TOPICS);
        ctx.deleteTopic(deleteTopicsRequestContext, createTopicResult.topicId());

        assertFalse(replicationControl.brokersToElrs().partitionsWithBrokerInElr(1).hasNext());
        assertFalse(replicationControl.brokersToIsrs().partitionsWithBrokerInIsr(0).hasNext());
    }

    @Test
    public void testEligibleLeaderReplicas_EffectiveMinIsr() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().setIsElrEnabled(true).build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][]{new int[]{0, 1, 2}});

        TopicIdPartition topicIdPartition = new TopicIdPartition(createTopicResult.topicId(), 0);
        assertEquals(OptionalInt.of(0), ctx.currentLeader(topicIdPartition));
        ctx.alterTopicConfig("foo", TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "5");
        assertEquals(3, replicationControl.getTopicEffectiveMinIsr("foo"));
    }

    @Test
    public void testEligibleLeaderReplicas_CleanElection() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
            .setIsElrEnabled(true)
            .build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3);
        ctx.unfenceBrokers(0, 1, 2, 3);
        CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2, 3}});

        TopicIdPartition topicIdPartition = new TopicIdPartition(createTopicResult.topicId(), 0);
        assertEquals(OptionalInt.of(0), ctx.currentLeader(topicIdPartition));
        ctx.alterTopicConfig("foo", TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3");

        ctx.fenceBrokers(Set.of(1, 2, 3));

        PartitionRegistration partition = replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        assertArrayEquals(new int[]{2, 3}, partition.elr, partition.toString());
        assertArrayEquals(new int[]{}, partition.lastKnownElr, partition.toString());

        ctx.unfenceBrokers(2);
        ctx.fenceBrokers(Set.of(0, 1));
        partition = replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        assertArrayEquals(new int[]{0, 3}, partition.elr, partition.toString());
        assertArrayEquals(new int[]{2}, partition.isr, partition.toString());
        assertEquals(2, partition.leader, partition.toString());
        assertArrayEquals(new int[]{}, partition.lastKnownElr, partition.toString());
    }

    @Test
    public void testEligibleLeaderReplicas_UncleanShutdown() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
            .setIsElrEnabled(true)
            .build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3);
        ctx.unfenceBrokers(0, 1, 2, 3);
        CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
                new int[][] {new int[] {0, 1, 2, 3}});

        TopicIdPartition topicIdPartition = new TopicIdPartition(createTopicResult.topicId(), 0);
        assertEquals(OptionalInt.of(0), ctx.currentLeader(topicIdPartition));
        ctx.alterTopicConfig("foo", TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3");

        ctx.fenceBrokers(Set.of(1, 2, 3));

        PartitionRegistration partition = replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        assertArrayEquals(new int[]{2, 3}, partition.elr, partition.toString());
        assertArrayEquals(new int[]{}, partition.lastKnownElr, partition.toString());

        // An unclean shutdown ELR member should be kicked out of ELR.
        ctx.handleBrokersUncleanShutdown(3);
        partition = replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        assertArrayEquals(new int[]{2}, partition.elr, partition.toString());
        assertArrayEquals(new int[]{}, partition.lastKnownElr, partition.toString());

        // An unclean shutdown last ISR member should be recognized as the last known leader.
        ctx.handleBrokersUncleanShutdown(0);
        partition = replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        assertArrayEquals(new int[]{2}, partition.elr, partition.toString());
        assertArrayEquals(new int[]{0}, partition.lastKnownElr, partition.toString());
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ALTER_PARTITION)
    public void testAlterPartitionHandleUnknownTopicIdOrName(short version) {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);

        String topicName = "foo";
        Uuid topicId = Uuid.randomUuid();

        AlterPartitionRequestData request = new AlterPartitionRequestData()
            .setBrokerId(0)
            .setBrokerEpoch(100)
            .setTopics(singletonList(new TopicData()
                .setTopicName(version <= 1 ? topicName : "")
                .setTopicId(version > 1 ? topicId : Uuid.ZERO_UUID)
                .setPartitions(singletonList(new PartitionData()
                    .setPartitionIndex(0)))));

        ControllerRequestContext requestContext =
            anonymousContextFor(ApiKeys.ALTER_PARTITION, version);

        ControllerResult<AlterPartitionResponseData> result =
            replicationControl.alterPartition(requestContext, request);

        Errors expectedError = version > 1 ? UNKNOWN_TOPIC_ID : UNKNOWN_TOPIC_OR_PARTITION;
        AlterPartitionResponseData expectedResponse = new AlterPartitionResponseData()
            .setTopics(singletonList(new AlterPartitionResponseData.TopicData()
                .setTopicName(version <= 1 ? topicName : "")
                .setTopicId(version > 1 ? topicId : Uuid.ZERO_UUID)
                .setPartitions(singletonList(new AlterPartitionResponseData.PartitionData()
                    .setPartitionIndex(0)
                    .setErrorCode(expectedError.code())))));

        assertEquals(expectedResponse, result.response());
    }

    @Test
    public void testInvalidAlterPartitionRequests() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
            new int[][] {new int[] {0, 1, 2}});

        TopicIdPartition topicIdPartition = new TopicIdPartition(createTopicResult.topicId(), 0);
        int leaderId = 0;
        int notLeaderId = 1;
        assertEquals(OptionalInt.of(leaderId), ctx.currentLeader(topicIdPartition));
        long brokerEpoch = ctx.currentBrokerEpoch(0);

        // Invalid leader
        PartitionData invalidLeaderRequest = newAlterPartition(
            replicationControl, topicIdPartition, isrWithDefaultEpoch(0, 1), LeaderRecoveryState.RECOVERED);
        ControllerResult<AlterPartitionResponseData> invalidLeaderResult = sendAlterPartition(
            replicationControl, notLeaderId, ctx.currentBrokerEpoch(notLeaderId),
            topicIdPartition.topicId(), invalidLeaderRequest);
        assertAlterPartitionResponse(invalidLeaderResult, topicIdPartition, Errors.INVALID_REQUEST);

        // Stale broker epoch
        PartitionData invalidBrokerEpochRequest = newAlterPartition(
            replicationControl, topicIdPartition, isrWithDefaultEpoch(0, 1), LeaderRecoveryState.RECOVERED);
        assertThrows(StaleBrokerEpochException.class, () -> sendAlterPartition(
            replicationControl, leaderId, brokerEpoch - 1, topicIdPartition.topicId(), invalidBrokerEpochRequest));

        // Invalid leader epoch
        PartitionData invalidLeaderEpochRequest = newAlterPartition(
            replicationControl, topicIdPartition, isrWithDefaultEpoch(0, 1), LeaderRecoveryState.RECOVERED);
        invalidLeaderEpochRequest.setLeaderEpoch(500);
        ControllerResult<AlterPartitionResponseData> invalidLeaderEpochResult = sendAlterPartition(
            replicationControl, leaderId, ctx.currentBrokerEpoch(leaderId),
            topicIdPartition.topicId(), invalidLeaderEpochRequest);
        assertAlterPartitionResponse(invalidLeaderEpochResult, topicIdPartition, NOT_CONTROLLER);

        // Invalid partition epoch
        PartitionData invalidPartitionEpochRequest = newAlterPartition(
            replicationControl, topicIdPartition, isrWithDefaultEpoch(0, 1), LeaderRecoveryState.RECOVERED);
        invalidPartitionEpochRequest.setPartitionEpoch(500);
        ControllerResult<AlterPartitionResponseData> invalidPartitionEpochResult = sendAlterPartition(
            replicationControl, leaderId, ctx.currentBrokerEpoch(leaderId),
            topicIdPartition.topicId(), invalidPartitionEpochRequest);
        assertAlterPartitionResponse(invalidPartitionEpochResult, topicIdPartition, NOT_CONTROLLER);

        // Invalid ISR (3 is not a valid replica)
        PartitionData invalidIsrRequest1 = newAlterPartition(
            replicationControl, topicIdPartition, isrWithDefaultEpoch(0, 1, 3), LeaderRecoveryState.RECOVERED);
        ControllerResult<AlterPartitionResponseData> invalidIsrResult1 = sendAlterPartition(
            replicationControl, leaderId, ctx.currentBrokerEpoch(leaderId),
            topicIdPartition.topicId(), invalidIsrRequest1);
        assertAlterPartitionResponse(invalidIsrResult1, topicIdPartition, Errors.INVALID_REQUEST);

        // Invalid ISR (does not include leader 0)
        PartitionData invalidIsrRequest2 = newAlterPartition(
            replicationControl, topicIdPartition, isrWithDefaultEpoch(1, 2), LeaderRecoveryState.RECOVERED);
        ControllerResult<AlterPartitionResponseData> invalidIsrResult2 = sendAlterPartition(
            replicationControl, leaderId, ctx.currentBrokerEpoch(leaderId),
            topicIdPartition.topicId(), invalidIsrRequest2);
        assertAlterPartitionResponse(invalidIsrResult2, topicIdPartition, Errors.INVALID_REQUEST);

        // Invalid ISR length and recovery state
        PartitionData invalidIsrRecoveryRequest = newAlterPartition(
            replicationControl, topicIdPartition, isrWithDefaultEpoch(0, 1), LeaderRecoveryState.RECOVERING);
        ControllerResult<AlterPartitionResponseData> invalidIsrRecoveryResult = sendAlterPartition(
            replicationControl, leaderId, ctx.currentBrokerEpoch(leaderId),
            topicIdPartition.topicId(), invalidIsrRecoveryRequest);
        assertAlterPartitionResponse(invalidIsrRecoveryResult, topicIdPartition, Errors.INVALID_REQUEST);

        // Invalid recovery state transition from RECOVERED to RECOVERING
        PartitionData invalidRecoveryRequest = newAlterPartition(
            replicationControl, topicIdPartition, isrWithDefaultEpoch(0), LeaderRecoveryState.RECOVERING);
        ControllerResult<AlterPartitionResponseData> invalidRecoveryResult = sendAlterPartition(
            replicationControl, leaderId, ctx.currentBrokerEpoch(leaderId),
            topicIdPartition.topicId(), invalidRecoveryRequest);
        assertAlterPartitionResponse(invalidRecoveryResult, topicIdPartition, Errors.INVALID_REQUEST);
    }

    private PartitionData newAlterPartition(
        ReplicationControlManager replicationControl,
        TopicIdPartition topicIdPartition,
        List<BrokerState> newIsrWithEpoch,
        LeaderRecoveryState leaderRecoveryState
    ) {
        PartitionRegistration partitionControl =
            replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        return new AlterPartitionRequestData.PartitionData()
            .setPartitionIndex(0)
            .setLeaderEpoch(partitionControl.leaderEpoch)
            .setPartitionEpoch(partitionControl.partitionEpoch)
            .setNewIsrWithEpochs(newIsrWithEpoch)
            .setLeaderRecoveryState(leaderRecoveryState.value());
    }

    private ControllerResult<AlterPartitionResponseData> sendAlterPartition(
        ReplicationControlManager replicationControl,
        int brokerId,
        long brokerEpoch,
        Uuid topicId,
        AlterPartitionRequestData.PartitionData partitionData
    ) {
        AlterPartitionRequestData request = new AlterPartitionRequestData()
            .setBrokerId(brokerId)
            .setBrokerEpoch(brokerEpoch);

        AlterPartitionRequestData.TopicData topicData = new AlterPartitionRequestData.TopicData()
            .setTopicId(topicId);
        request.topics().add(topicData);
        topicData.partitions().add(partitionData);

        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.ALTER_PARTITION);
        ControllerResult<AlterPartitionResponseData> result = replicationControl.alterPartition(requestContext, request);
        RecordTestUtils.replayAll(replicationControl, result.records());
        return result;
    }

    private AlterPartitionResponseData.PartitionData assertAlterPartitionResponse(
        ControllerResult<AlterPartitionResponseData> alterPartitionResult,
        TopicIdPartition topicIdPartition,
        Errors expectedError
    ) {
        AlterPartitionResponseData response = alterPartitionResult.response();
        assertEquals(1, response.topics().size());

        AlterPartitionResponseData.TopicData topicData = response.topics().get(0);
        assertEquals(topicIdPartition.topicId(), topicData.topicId());
        assertEquals(1, topicData.partitions().size());

        AlterPartitionResponseData.PartitionData partitionData = topicData.partitions().get(0);
        assertEquals(topicIdPartition.partitionId(), partitionData.partitionIndex());
        assertEquals(expectedError, Errors.forCode(partitionData.errorCode()));
        return partitionData;
    }

    private void assertConsistentAlterPartitionResponse(
        ReplicationControlManager replicationControl,
        TopicIdPartition topicIdPartition,
        AlterPartitionResponseData.PartitionData partitionData
    ) {
        PartitionRegistration partitionControl =
            replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        assertEquals(partitionControl.leader, partitionData.leaderId());
        assertEquals(partitionControl.leaderEpoch, partitionData.leaderEpoch());
        assertEquals(partitionControl.partitionEpoch, partitionData.partitionEpoch());
        List<Integer> expectedIsr = IntStream.of(partitionControl.isr).boxed().collect(Collectors.toList());
        assertEquals(expectedIsr, partitionData.isr());
    }

    private void assertCreatedTopicConfigs(
        ReplicationControlTestContext ctx,
        String topic,
        CreateTopicsRequestData.CreatableTopicConfigCollection requestConfigs
    ) {
        Map<String, String> configs = ctx.configurationControl.getConfigs(
            new ConfigResource(ConfigResource.Type.TOPIC, topic));
        assertEquals(requestConfigs.size(), configs.size());
        for (CreateTopicsRequestData.CreatableTopicConfig requestConfig : requestConfigs) {
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
    public void testDeleteTopics() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        CreateTopicsRequestData.CreatableTopicConfigCollection requestConfigs =
            new CreateTopicsRequestData.CreatableTopicConfigCollection();
        requestConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig().
            setName("cleanup.policy").setValue("compact"));
        requestConfigs.add(new CreateTopicsRequestData.CreatableTopicConfig().
            setName("min.cleanable.dirty.ratio").setValue("0.1"));
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(3).setReplicationFactor((short) 2).
            setConfigs(requestConfigs));
        ctx.registerBrokers(0, 1);
        ctx.unfenceBrokers(0, 1);
        ControllerRequestContext createTopicsRequestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> createResult =
            replicationControl.createTopics(createTopicsRequestContext, request, Collections.singleton("foo"));
        CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
        Uuid topicId = createResult.response().topics().find("foo").topicId();
        expectedResponse.topics().add(new CreatableTopicResult().setName("foo").
            setNumPartitions(3).setReplicationFactor((short) 2).
            setErrorMessage(null).setErrorCode((short) 0).
            setTopicId(topicId));
        assertEquals(expectedResponse, withoutConfigs(createResult.response()));
        // Until the records are replayed, no changes are made
        assertNull(replicationControl.getPartition(topicId, 0));
        assertEmptyTopicConfigs(ctx, "foo");
        ctx.replay(createResult.records());
        assertNotNull(replicationControl.getPartition(topicId, 0));
        assertNotNull(replicationControl.getPartition(topicId, 1));
        assertNotNull(replicationControl.getPartition(topicId, 2));
        assertNull(replicationControl.getPartition(topicId, 3));
        assertCreatedTopicConfigs(ctx, "foo", requestConfigs);

        assertEquals(singletonMap(topicId, new ResultOrError<>("foo")),
            replicationControl.findTopicNames(Long.MAX_VALUE, Collections.singleton(topicId)));
        assertEquals(singletonMap("foo", new ResultOrError<>(topicId)),
            replicationControl.findTopicIds(Long.MAX_VALUE, Collections.singleton("foo")));
        Uuid invalidId = new Uuid(topicId.getMostSignificantBits() + 1,
            topicId.getLeastSignificantBits());
        assertEquals(singletonMap(invalidId,
            new ResultOrError<>(new ApiError(UNKNOWN_TOPIC_ID))),
                replicationControl.findTopicNames(Long.MAX_VALUE, Collections.singleton(invalidId)));
        assertEquals(singletonMap("bar",
            new ResultOrError<>(new ApiError(UNKNOWN_TOPIC_OR_PARTITION))),
                replicationControl.findTopicIds(Long.MAX_VALUE, Collections.singleton("bar")));

        ControllerRequestContext deleteTopicsRequestContext = anonymousContextFor(ApiKeys.DELETE_TOPICS);
        ControllerResult<Map<Uuid, ApiError>> invalidDeleteResult = replicationControl.
            deleteTopics(deleteTopicsRequestContext, Collections.singletonList(invalidId));
        assertEquals(0, invalidDeleteResult.records().size());
        assertEquals(singletonMap(invalidId, new ApiError(UNKNOWN_TOPIC_ID, null)),
            invalidDeleteResult.response());
        ControllerResult<Map<Uuid, ApiError>> deleteResult = replicationControl.
            deleteTopics(deleteTopicsRequestContext, Collections.singletonList(topicId));
        assertTrue(deleteResult.isAtomic());
        assertEquals(singletonMap(topicId, new ApiError(NONE, null)),
            deleteResult.response());
        assertEquals(1, deleteResult.records().size());
        ctx.replay(deleteResult.records());
        assertNull(replicationControl.getPartition(topicId, 0));
        assertNull(replicationControl.getPartition(topicId, 1));
        assertNull(replicationControl.getPartition(topicId, 2));
        assertNull(replicationControl.getPartition(topicId, 3));
        assertEquals(singletonMap(topicId, new ResultOrError<>(
            new ApiError(UNKNOWN_TOPIC_ID))), replicationControl.findTopicNames(
                Long.MAX_VALUE, Collections.singleton(topicId)));
        assertEquals(singletonMap("foo", new ResultOrError<>(
            new ApiError(UNKNOWN_TOPIC_OR_PARTITION))), replicationControl.findTopicIds(
                Long.MAX_VALUE, Collections.singleton("foo")));
        assertEmptyTopicConfigs(ctx, "foo");
    }

    @Test
    public void testDeleteTopicsWithMutationQuotaExceeded() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(3).setReplicationFactor((short) 2));
        ctx.registerBrokers(0, 1);
        ctx.unfenceBrokers(0, 1);
        ControllerRequestContext createTopicsRequestContext =
            anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> createResult =
            replicationControl.createTopics(createTopicsRequestContext, request, Collections.singleton("foo"));
        CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
        CreatableTopicResult createdTopic = createResult.response().topics().find("foo");
        assertEquals(NONE.code(), createdTopic.errorCode());
        ctx.replay(createResult.records());
        ControllerRequestContext deleteTopicsRequestContext =
            anonymousContextWithMutationQuotaExceededFor(ApiKeys.DELETE_TOPICS);
        Uuid topicId = createdTopic.topicId();
        ControllerResult<Map<Uuid, ApiError>> deleteResult = replicationControl.
            deleteTopics(deleteTopicsRequestContext, Collections.singletonList(topicId));
        assertEquals(singletonMap(topicId, new ApiError(THROTTLING_QUOTA_EXCEEDED, QUOTA_EXCEEDED_IN_TEST_MSG)),
            deleteResult.response());
        assertEquals(0, deleteResult.records().size());
    }

    @Test
    public void testCreatePartitions() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(3).setReplicationFactor((short) 2));
        request.topics().add(new CreatableTopic().setName("bar").
            setNumPartitions(4).setReplicationFactor((short) 2));
        request.topics().add(new CreatableTopic().setName("quux").
            setNumPartitions(2).setReplicationFactor((short) 2));
        request.topics().add(new CreatableTopic().setName("foo2").
            setNumPartitions(2).setReplicationFactor((short) 2));
        ctx.registerBrokersWithDirs(
                0, Collections.emptyList(),
                1, asList(Uuid.fromString("QMzamNQVQ7GnJK9DwQHG7Q"), Uuid.fromString("loDxEBLETdedNnQGOKKENw")),
                3, Collections.singletonList(Uuid.fromString("dxCDSgNjQvS4WuyqEKoCwA")));
        ctx.unfenceBrokers(0, 1, 3);
        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> createTopicResult = replicationControl.
            createTopics(requestContext, request, new HashSet<>(Arrays.asList("foo", "bar", "quux", "foo2")));
        ctx.replay(createTopicResult.records());
        List<CreatePartitionsTopic> topics = new ArrayList<>();
        topics.add(new CreatePartitionsTopic().
            setName("foo").setCount(5).setAssignments(null));
        topics.add(new CreatePartitionsTopic().
            setName("bar").setCount(3).setAssignments(null));
        topics.add(new CreatePartitionsTopic().
            setName("baz").setCount(3).setAssignments(null));
        topics.add(new CreatePartitionsTopic().
            setName("quux").setCount(2).setAssignments(null));
        ControllerResult<List<CreatePartitionsTopicResult>> createPartitionsResult =
            replicationControl.createPartitions(requestContext, topics);
        assertEquals(asList(new CreatePartitionsTopicResult().
                setName("foo").
                setErrorCode(NONE.code()).
                setErrorMessage(null),
            new CreatePartitionsTopicResult().
                setName("bar").
                setErrorCode(INVALID_PARTITIONS.code()).
                setErrorMessage("The topic bar currently has 4 partition(s); 3 would not be an increase."),
            new CreatePartitionsTopicResult().
                setName("baz").
                setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code()).
                setErrorMessage(null),
            new CreatePartitionsTopicResult().
                setName("quux").
                setErrorCode(INVALID_PARTITIONS.code()).
                setErrorMessage("Topic already has 2 partition(s).")),
            createPartitionsResult.response());
        ctx.replay(createPartitionsResult.records());
        List<CreatePartitionsTopic> topics2 = new ArrayList<>();
        topics2.add(new CreatePartitionsTopic().
            setName("foo").setCount(6).setAssignments(singletonList(
                new CreatePartitionsAssignment().setBrokerIds(asList(1, 3)))));
        topics2.add(new CreatePartitionsTopic().
            setName("bar").setCount(5).setAssignments(singletonList(
                new CreatePartitionsAssignment().setBrokerIds(singletonList(1)))));
        topics2.add(new CreatePartitionsTopic().
            setName("quux").setCount(4).setAssignments(singletonList(
                new CreatePartitionsAssignment().setBrokerIds(asList(1, 0)))));
        topics2.add(new CreatePartitionsTopic().
            setName("foo2").setCount(3).setAssignments(singletonList(
                new CreatePartitionsAssignment().setBrokerIds(asList(2, 0)))));
        ControllerResult<List<CreatePartitionsTopicResult>> createPartitionsResult2 =
            replicationControl.createPartitions(requestContext, topics2);
        assertEquals(asList(new CreatePartitionsTopicResult().
                setName("foo").
                setErrorCode(NONE.code()).
                setErrorMessage(null),
            new CreatePartitionsTopicResult().
                setName("bar").
                setErrorCode(INVALID_REPLICA_ASSIGNMENT.code()).
                setErrorMessage("The manual partition assignment includes a partition " +
                    "with 1 replica(s), but this is not consistent with previous " +
                    "partitions, which have 2 replica(s)."),
            new CreatePartitionsTopicResult().
                setName("quux").
                setErrorCode(INVALID_REPLICA_ASSIGNMENT.code()).
                setErrorMessage("Attempted to add 2 additional partition(s), but only 1 assignment(s) were specified."),
            new CreatePartitionsTopicResult().
                setName("foo2").
                setErrorCode(INVALID_REPLICA_ASSIGNMENT.code()).
                setErrorMessage("The manual partition assignment includes broker 2, but " +
                    "no such broker is registered.")),
            createPartitionsResult2.response());
        ctx.replay(createPartitionsResult2.records());
        assertArrayEquals(
                new Uuid[] {DirectoryId.UNASSIGNED, Uuid.fromString("dxCDSgNjQvS4WuyqEKoCwA")},
                replicationControl.getPartition(replicationControl.getTopicId("foo"), 5).directories);
    }

    @Test
    public void testCreatePartitionsWithMutationQuotaExceeded() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(3).setReplicationFactor((short) 2));
        ctx.registerBrokers(0, 1);
        ctx.unfenceBrokers(0, 1);
        ControllerRequestContext createTopicsRequestContext =
            anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> createResult =
            replicationControl.createTopics(createTopicsRequestContext, request, Collections.singleton("foo"));
        CreatableTopicResult createdTopic = createResult.response().topics().find("foo");
        assertEquals(NONE.code(), createdTopic.errorCode());
        ctx.replay(createResult.records());
        List<CreatePartitionsTopic> topics = new ArrayList<>();
        topics.add(new CreatePartitionsTopic().
            setName("foo").setCount(5).setAssignments(null));
        ControllerRequestContext createPartitionsRequestContext =
            anonymousContextWithMutationQuotaExceededFor(ApiKeys.CREATE_PARTITIONS);
        ControllerResult<List<CreatePartitionsTopicResult>> createPartitionsResult =
            replicationControl.createPartitions(createPartitionsRequestContext, topics);
        List<CreatePartitionsTopicResult> expectedThrottled = singletonList(new CreatePartitionsTopicResult().
            setName("foo").
            setErrorCode(THROTTLING_QUOTA_EXCEEDED.code()).
            setErrorMessage(QUOTA_EXCEEDED_IN_TEST_MSG));
        assertEquals(expectedThrottled, createPartitionsResult.response());
        // now test the explicit assignment case
        List<CreatePartitionsTopic> topics2 = new ArrayList<>();
        topics2.add(new CreatePartitionsTopic().
            setName("foo").setCount(4).setAssignments(singletonList(
                new CreatePartitionsAssignment().setBrokerIds(asList(1, 0)))));
        ControllerResult<List<CreatePartitionsTopicResult>> createPartitionsResult2 =
            replicationControl.createPartitions(createPartitionsRequestContext, topics2);
        assertEquals(expectedThrottled, createPartitionsResult2.response());
    }

    @Test
    public void testCreatePartitionsFailsWhenAllBrokersAreFencedOrInControlledShutdown() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(1).setReplicationFactor((short) 2));

        ctx.registerBrokers(0, 1);
        ctx.unfenceBrokers(0, 1);

        ControllerRequestContext requestContext =
                anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> createTopicResult = replicationControl.
            createTopics(requestContext, request, new HashSet<>(singletonList("foo")));
        ctx.replay(createTopicResult.records());

        ctx.registerBrokers(0, 1);
        ctx.unfenceBrokers(0);
        ctx.inControlledShutdownBrokers(0);

        List<CreatePartitionsTopic> topics = new ArrayList<>();
        topics.add(new CreatePartitionsTopic().
            setName("foo").setCount(2).setAssignments(null));
        ControllerResult<List<CreatePartitionsTopicResult>> createPartitionsResult =
            replicationControl.createPartitions(requestContext, topics);

        assertEquals(
            singletonList(new CreatePartitionsTopicResult().
                setName("foo").
                setErrorCode(INVALID_REPLICATION_FACTOR.code()).
                setErrorMessage("Unable to replicate the partition 2 time(s): All " +
                    "brokers are currently fenced or in controlled shutdown.")),
            createPartitionsResult.response());
    }

    @Test
    public void testCreatePartitionsISRInvariants() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;

        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(1).setReplicationFactor((short) 3));

        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1);
        ctx.inControlledShutdownBrokers(1);

        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> result =
            replicationControl.createTopics(requestContext, request, Collections.singleton("foo"));
        ctx.replay(result.records());

        List<CreatePartitionsTopic> topics = singletonList(new CreatePartitionsTopic().
            setName("foo").setCount(2).setAssignments(null));

        ControllerResult<List<CreatePartitionsTopicResult>> createPartitionsResult =
            replicationControl.createPartitions(requestContext, topics);
        ctx.replay(createPartitionsResult.records());

        // Broker 2 cannot be in the ISR because it is fenced and broker 1
        // cannot be in the ISR because it is in controlled shutdown.
        assertEquals(
            new PartitionRegistration.Builder().setReplicas(new int[]{0, 1, 2}).
                setDirectories(new Uuid[] {
                    Uuid.fromString("TESTBROKER00000DIRAAAA"),
                    Uuid.fromString("TESTBROKER00001DIRAAAA"),
                    Uuid.fromString("TESTBROKER00002DIRAAAA")
                }).
                setIsr(new int[]{0}).
                setLeader(0).
                setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                setLeaderEpoch(0).
                setPartitionEpoch(0).
                build(),
            replicationControl.getPartition(
                ((TopicRecord) result.records().get(0).message()).topicId(), 1));
    }

    @Test
    public void testValidateGoodManualPartitionAssignments() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(1, 2, 3);
        ctx.replicationControl.validateManualPartitionAssignment(partitionAssignment(singletonList(1)),
            OptionalInt.of(1));
        ctx.replicationControl.validateManualPartitionAssignment(partitionAssignment(singletonList(1)),
            OptionalInt.empty());
        ctx.replicationControl.validateManualPartitionAssignment(partitionAssignment(asList(1, 2, 3)),
            OptionalInt.of(3));
        ctx.replicationControl.validateManualPartitionAssignment(partitionAssignment(asList(1, 2, 3)),
            OptionalInt.empty());
    }

    @Test
    public void testValidateBadManualPartitionAssignments() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(1, 2);
        assertEquals("The manual partition assignment includes an empty replica list.",
            assertThrows(InvalidReplicaAssignmentException.class, () ->
                ctx.replicationControl.validateManualPartitionAssignment(partitionAssignment(Collections.emptyList()),
                    OptionalInt.empty())).getMessage());
        assertEquals("The manual partition assignment includes broker 3, but no such " +
            "broker is registered.", assertThrows(InvalidReplicaAssignmentException.class, () ->
                ctx.replicationControl.validateManualPartitionAssignment(partitionAssignment(asList(1, 2, 3)),
                    OptionalInt.empty())).getMessage());
        assertEquals("The manual partition assignment includes the broker 2 more than " +
            "once.", assertThrows(InvalidReplicaAssignmentException.class, () ->
                ctx.replicationControl.validateManualPartitionAssignment(partitionAssignment(asList(1, 2, 2)),
                    OptionalInt.empty())).getMessage());
        assertEquals("The manual partition assignment includes a partition with 2 " +
            "replica(s), but this is not consistent with previous partitions, which have " +
                "3 replica(s).", assertThrows(InvalidReplicaAssignmentException.class, () ->
                    ctx.replicationControl.validateManualPartitionAssignment(partitionAssignment(asList(1, 2)),
                        OptionalInt.of(3))).getMessage());
    }

    private static final ListPartitionReassignmentsResponseData NONE_REASSIGNING =
        new ListPartitionReassignmentsResponseData().setErrorMessage(null);

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ALTER_PARTITION)
    public void testReassignPartitions(short version) {
        MetadataVersion metadataVersion = MetadataVersion.latestTesting();
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3);
        ctx.unfenceBrokers(0, 1, 2, 3);
        Uuid fooId = ctx.createTestTopic("foo", new int[][] {
            new int[] {1, 2, 3}, new int[] {3, 2, 1}}).topicId();
        ctx.createTestTopic("bar", new int[][] {
            new int[] {1, 2, 3}}).topicId();
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(null, Long.MAX_VALUE));
        ControllerResult<AlterPartitionReassignmentsResponseData> alterResult =
            replication.alterPartitionReassignments(
                new AlterPartitionReassignmentsRequestData().setTopics(asList(
                    new ReassignableTopic().setName("foo").setPartitions(asList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(asList(3, 2, 1)),
                        new ReassignablePartition().setPartitionIndex(1).
                            setReplicas(asList(0, 2, 1)),
                        new ReassignablePartition().setPartitionIndex(2).
                            setReplicas(asList(0, 2, 1)))),
                    new ReassignableTopic().setName("bar"))));
        assertEquals(new AlterPartitionReassignmentsResponseData().
                setErrorMessage(null).setResponses(asList(
                    new ReassignableTopicResponse().setName("foo").setPartitions(asList(
                        new ReassignablePartitionResponse().setPartitionIndex(0).
                            setErrorMessage(null),
                        new ReassignablePartitionResponse().setPartitionIndex(1).
                            setErrorMessage(null),
                        new ReassignablePartitionResponse().setPartitionIndex(2).
                            setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code()).
                            setErrorMessage("Unable to find partition foo:2."))),
                    new ReassignableTopicResponse().
                        setName("bar"))),
            alterResult.response());
        ctx.replay(alterResult.records());
        ListPartitionReassignmentsResponseData currentReassigning =
            new ListPartitionReassignmentsResponseData().setErrorMessage(null).
                setTopics(singletonList(new OngoingTopicReassignment().
                    setName("foo").setPartitions(singletonList(
                        new OngoingPartitionReassignment().setPartitionIndex(1).
                            setRemovingReplicas(singletonList(3)).
                            setAddingReplicas(singletonList(0)).
                            setReplicas(asList(0, 2, 1, 3))))));
        assertEquals(currentReassigning, replication.listPartitionReassignments(null, Long.MAX_VALUE));
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(singletonList(
            new ListPartitionReassignmentsTopics().setName("bar").
                setPartitionIndexes(asList(0, 1, 2))), Long.MAX_VALUE));
        assertEquals(currentReassigning, replication.listPartitionReassignments(singletonList(
            new ListPartitionReassignmentsTopics().setName("foo").
                setPartitionIndexes(asList(0, 1, 2))), Long.MAX_VALUE));
        ControllerResult<AlterPartitionReassignmentsResponseData> cancelResult =
            replication.alterPartitionReassignments(
                new AlterPartitionReassignmentsRequestData().setTopics(asList(
                    new ReassignableTopic().setName("foo").setPartitions(asList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(null),
                        new ReassignablePartition().setPartitionIndex(1).
                            setReplicas(null),
                        new ReassignablePartition().setPartitionIndex(2).
                            setReplicas(null))),
                    new ReassignableTopic().setName("bar").setPartitions(singletonList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(null))))));
        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
            new PartitionChangeRecord().setTopicId(fooId).
                setPartitionId(1).
                setReplicas(asList(2, 1, 3)).
                setDirectories(asList(
                        Uuid.fromString("TESTBROKER00002DIRAAAA"),
                        Uuid.fromString("TESTBROKER00001DIRAAAA"),
                        Uuid.fromString("TESTBROKER00003DIRAAAA")
                )).
                setLeader(3).
                setRemovingReplicas(Collections.emptyList()).
                setAddingReplicas(Collections.emptyList()), MetadataVersion.latestTesting().partitionChangeRecordVersion())),
            new AlterPartitionReassignmentsResponseData().setErrorMessage(null).setResponses(asList(
                new ReassignableTopicResponse().setName("foo").setPartitions(asList(
                    new ReassignablePartitionResponse().setPartitionIndex(0).
                        setErrorCode(NO_REASSIGNMENT_IN_PROGRESS.code()).setErrorMessage(null),
                    new ReassignablePartitionResponse().setPartitionIndex(1).
                        setErrorCode(NONE.code()).setErrorMessage(null),
                    new ReassignablePartitionResponse().setPartitionIndex(2).
                        setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code()).
                        setErrorMessage("Unable to find partition foo:2."))),
                new ReassignableTopicResponse().setName("bar").setPartitions(singletonList(
                    new ReassignablePartitionResponse().setPartitionIndex(0).
                        setErrorCode(NO_REASSIGNMENT_IN_PROGRESS.code()).
                        setErrorMessage(null)))))),
            cancelResult);
        log.info("running final alterPartition...");
        ControllerRequestContext requestContext =
            anonymousContextFor(ApiKeys.ALTER_PARTITION, version);
        AlterPartitionRequestData alterPartitionRequestData = new AlterPartitionRequestData().
                setBrokerId(3).
                setBrokerEpoch(103).
                setTopics(singletonList(new TopicData().
                    setTopicName(version <= 1 ? "foo" : "").
                    setTopicId(version > 1 ? fooId : Uuid.ZERO_UUID).
                    setPartitions(singletonList(new PartitionData().
                        setPartitionIndex(1).
                        setPartitionEpoch(1).
                        setLeaderEpoch(0).
                        setNewIsrWithEpochs(isrWithDefaultEpoch(3, 0, 2, 1))))));
        ControllerResult<AlterPartitionResponseData> alterPartitionResult = replication.alterPartition(
            requestContext,
            new AlterPartitionRequest.Builder(alterPartitionRequestData, version > 1).build(version).data());
        Errors expectedError = version > 1 ? NEW_LEADER_ELECTED : FENCED_LEADER_EPOCH;
        assertEquals(new AlterPartitionResponseData().setTopics(singletonList(
            new AlterPartitionResponseData.TopicData().
                setTopicName(version <= 1 ? "foo" : "").
                setTopicId(version > 1 ? fooId : Uuid.ZERO_UUID).
                setPartitions(singletonList(
                    new AlterPartitionResponseData.PartitionData().
                        setPartitionIndex(1).
                        setErrorCode(expectedError.code()))))),
            alterPartitionResult.response());
        ctx.replay(alterPartitionResult.records());
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(null, Long.MAX_VALUE));
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ALTER_PARTITION)
    public void testAlterPartitionShouldRejectFencedBrokers(short version) {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(0, 1, 2, 3, 4);
        Uuid fooId = ctx.createTestTopic(
            "foo",
            new int[][] {new int[] {1, 2, 3, 4}}
        ).topicId();

        List<ApiMessageAndVersion> fenceRecords = new ArrayList<>();
        replication.handleBrokerFenced(3, fenceRecords);
        ctx.replay(fenceRecords);

        assertEquals(
            new PartitionRegistration.Builder().
                setReplicas(new int[] {1, 2, 3, 4}).
                setDirectories(new Uuid[] {
                        Uuid.fromString("TESTBROKER00001DIRAAAA"),
                        Uuid.fromString("TESTBROKER00002DIRAAAA"),
                        Uuid.fromString("TESTBROKER00003DIRAAAA"),
                        Uuid.fromString("TESTBROKER00004DIRAAAA")
                }).
                setIsr(new int[] {1, 2, 4}).
                setLeader(1).
                setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                setLeaderEpoch(0).
                setPartitionEpoch(1).
                build(),
            replication.getPartition(fooId, 0));

        AlterPartitionRequestData alterIsrRequest = new AlterPartitionRequestData()
            .setBrokerId(1)
            .setBrokerEpoch(101)
            .setTopics(singletonList(new TopicData()
                .setTopicName(version <= 1 ? "foo" : "")
                .setTopicId(version > 1 ? fooId : Uuid.ZERO_UUID)
                .setPartitions(singletonList(new PartitionData()
                    .setPartitionIndex(0)
                    .setPartitionEpoch(1)
                    .setLeaderEpoch(0)
                    .setNewIsrWithEpochs(isrWithDefaultEpoch(1, 2, 3, 4))))));

        ControllerRequestContext requestContext =
            anonymousContextFor(ApiKeys.ALTER_PARTITION, version);

        ControllerResult<AlterPartitionResponseData> alterPartitionResult =
            replication.alterPartition(requestContext, new AlterPartitionRequest.Builder(alterIsrRequest, version > 1).build(version).data());

        Errors expectedError = version <= 1 ? OPERATION_NOT_ATTEMPTED : INELIGIBLE_REPLICA;
        assertEquals(
            new AlterPartitionResponseData()
                .setTopics(singletonList(new AlterPartitionResponseData.TopicData()
                    .setTopicName(version <= 1 ? "foo" : "")
                    .setTopicId(version > 1 ? fooId : Uuid.ZERO_UUID)
                    .setPartitions(singletonList(new AlterPartitionResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setErrorCode(expectedError.code()))))),
            alterPartitionResult.response());

        fenceRecords = new ArrayList<>();
        replication.handleBrokerUnfenced(3, 103, fenceRecords);
        ctx.replay(fenceRecords);

        alterPartitionResult = replication.alterPartition(requestContext, alterIsrRequest);

        assertEquals(
            new AlterPartitionResponseData()
                .setTopics(singletonList(new AlterPartitionResponseData.TopicData()
                    .setTopicName(version <= 1 ? "foo" : "")
                    .setTopicId(version > 1 ? fooId : Uuid.ZERO_UUID)
                    .setPartitions(singletonList(new AlterPartitionResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setLeaderId(1)
                        .setLeaderEpoch(0)
                        .setIsr(asList(1, 2, 3, 4))
                        .setPartitionEpoch(2)
                        .setErrorCode(NONE.code()))))),
            alterPartitionResult.response());
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ALTER_PARTITION)
    public void testAlterPartitionShouldRejectBrokersWithStaleEpoch(short version) {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(0, 1, 2, 3, 4);
        Uuid fooId = ctx.createTestTopic(
            "foo",
            new int[][] {new int[] {1, 2, 3, 4}}
        ).topicId();
        ctx.alterPartition(new TopicIdPartition(fooId, 0), 1, isrWithDefaultEpoch(1, 2, 3), LeaderRecoveryState.RECOVERED);

        // First, the leader is constructing an AlterPartition request.
        AlterPartitionRequestData alterIsrRequest = new AlterPartitionRequestData().
            setBrokerId(1).
            setBrokerEpoch(101).
            setTopics(singletonList(new TopicData().
                setTopicName(version <= 1 ? "foo" : "").
                setTopicId(version > 1 ? fooId : Uuid.ZERO_UUID).
                setPartitions(singletonList(new PartitionData().
                    setPartitionIndex(0).
                    setPartitionEpoch(1).
                    setLeaderEpoch(0).
                    setNewIsrWithEpochs(isrWithDefaultEpoch(1, 2, 3, 4))))));

        // The broker 4 has failed silently and now registers again.
        long newEpoch = defaultBrokerEpoch(4) + 1000;
        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().
            setBrokerEpoch(newEpoch).setBrokerId(4).setRack(null);
        brokerRecord.endPoints().add(new RegisterBrokerRecord.BrokerEndpoint().
            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
            setPort((short) 9092 + 4).
            setName("PLAINTEXT").
            setHost("localhost"));
        ctx.replay(Collections.singletonList(new ApiMessageAndVersion(brokerRecord, (short) 0)));

        // Unfence the broker 4.
        ControllerResult<BrokerHeartbeatReply> result = ctx.replicationControl.
            processBrokerHeartbeat(new BrokerHeartbeatRequestData().
                setBrokerId(4).setBrokerEpoch(newEpoch).
                setCurrentMetadataOffset(1).
                setWantFence(false).setWantShutDown(false), 0);
        assertEquals(new BrokerHeartbeatReply(true, false, false, false),
            result.response());
        ctx.replay(result.records());

        ControllerRequestContext requestContext =
            anonymousContextFor(ApiKeys.ALTER_PARTITION, version);

        ControllerResult<AlterPartitionResponseData> alterPartitionResult =
            replication.alterPartition(requestContext, new AlterPartitionRequest.Builder(alterIsrRequest, version > 1).build(version).data());

        // The late arrived AlterPartition request should be rejected when version >= 3.
        if (version >= 3) {
            assertEquals(
                new AlterPartitionResponseData().
                    setTopics(singletonList(new AlterPartitionResponseData.TopicData().
                        setTopicName("").
                        setTopicId(fooId).
                        setPartitions(singletonList(new AlterPartitionResponseData.PartitionData().
                            setPartitionIndex(0).
                            setErrorCode(INELIGIBLE_REPLICA.code()))))),
                alterPartitionResult.response());
        } else {
            assertEquals(NONE.code(), alterPartitionResult.response().errorCode());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ALTER_PARTITION)
    public void testAlterPartitionShouldRejectShuttingDownBrokers(short version) {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(0, 1, 2, 3, 4);
        Uuid fooId = ctx.createTestTopic(
            "foo",
            new int[][] {new int[] {1, 2, 3, 4}}
        ).topicId();

        assertEquals(
            new PartitionRegistration.Builder().
                setReplicas(new int[] {1, 2, 3, 4}).
                setDirectories(new Uuid[] {
                        Uuid.fromString("TESTBROKER00001DIRAAAA"),
                        Uuid.fromString("TESTBROKER00002DIRAAAA"),
                        Uuid.fromString("TESTBROKER00003DIRAAAA"),
                        Uuid.fromString("TESTBROKER00004DIRAAAA")
                }).
                setIsr(new int[] {1, 2, 3, 4}).
                setLeader(1).
                setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                setLeaderEpoch(0).
                setPartitionEpoch(0).
                build(),
            replication.getPartition(fooId, 0));

        ctx.inControlledShutdownBrokers(3);

        AlterPartitionRequestData alterIsrRequest = new AlterPartitionRequestData()
            .setBrokerId(1)
            .setBrokerEpoch(101)
            .setTopics(singletonList(new TopicData()
                .setTopicName(version <= 1 ? "foo" : "")
                .setTopicId(version > 1 ? fooId : Uuid.ZERO_UUID)
                .setPartitions(singletonList(new PartitionData()
                    .setPartitionIndex(0)
                    .setPartitionEpoch(0)
                    .setLeaderEpoch(0)
                    .setNewIsrWithEpochs(isrWithDefaultEpoch(1, 2, 3, 4))))));

        ControllerRequestContext requestContext =
            anonymousContextFor(ApiKeys.ALTER_PARTITION, version);

        ControllerResult<AlterPartitionResponseData> alterPartitionResult =
            replication.alterPartition(requestContext, new AlterPartitionRequest.Builder(alterIsrRequest, version > 1).build(version).data());

        Errors expectedError = version <= 1 ? OPERATION_NOT_ATTEMPTED : INELIGIBLE_REPLICA;
        assertEquals(
            new AlterPartitionResponseData()
                .setTopics(singletonList(new AlterPartitionResponseData.TopicData()
                    .setTopicName(version <= 1 ? "foo" : "")
                    .setTopicId(version > 1 ? fooId : Uuid.ZERO_UUID)
                    .setPartitions(singletonList(new AlterPartitionResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setErrorCode(expectedError.code()))))),
            alterPartitionResult.response());
    }

    @Test
    public void testCancelReassignPartitions() {
        MetadataVersion metadataVersion = MetadataVersion.latestTesting();
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder()
                .setMetadataVersion(metadataVersion)
                .build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(0, 1, 2, 3, 4);
        Uuid fooId = ctx.createTestTopic("foo", new int[][] {
            new int[] {1, 2, 3, 4}, new int[] {0, 1, 2, 3}, new int[] {4, 3, 1, 0},
            new int[] {2, 3, 4, 1}}).topicId();
        Uuid barId = ctx.createTestTopic("bar", new int[][] {
            new int[] {4, 3, 2}}).topicId();
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(null, Long.MAX_VALUE));
        List<ApiMessageAndVersion> fenceRecords = new ArrayList<>();
        replication.handleBrokerFenced(3, fenceRecords);
        ctx.replay(fenceRecords);
        assertEquals(new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4}).setIsr(new int[] {1, 2, 4}).
            setDirectories(new Uuid[] {
                    Uuid.fromString("TESTBROKER00001DIRAAAA"),
                    Uuid.fromString("TESTBROKER00002DIRAAAA"),
                    Uuid.fromString("TESTBROKER00003DIRAAAA"),
                    Uuid.fromString("TESTBROKER00004DIRAAAA")
            }).
            setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(0).setPartitionEpoch(1).build(), replication.getPartition(fooId, 0));
        ControllerResult<AlterPartitionReassignmentsResponseData> alterResult =
            replication.alterPartitionReassignments(
                new AlterPartitionReassignmentsRequestData().setTopics(asList(
                    new ReassignableTopic().setName("foo").setPartitions(asList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(asList(1, 2, 4)),
                        new ReassignablePartition().setPartitionIndex(1).
                            setReplicas(asList(1, 2, 3, 0)),
                        new ReassignablePartition().setPartitionIndex(2).
                            setReplicas(asList(5, 6, 7)),
                        new ReassignablePartition().setPartitionIndex(3).
                            setReplicas(Collections.emptyList()))),
                    new ReassignableTopic().setName("bar").setPartitions(singletonList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(asList(1, 2, 3, 4, 0)))))));
        assertEquals(new AlterPartitionReassignmentsResponseData().
                setErrorMessage(null).
                setResponses(asList(
                    new ReassignableTopicResponse().setName("foo").setPartitions(asList(
                        new ReassignablePartitionResponse().setPartitionIndex(0).setErrorMessage(null), 
                        new ReassignablePartitionResponse().setPartitionIndex(1).setErrorMessage(null), 
                        new ReassignablePartitionResponse().setPartitionIndex(2).setErrorCode(INVALID_REPLICA_ASSIGNMENT.code()).
                            setErrorMessage("The manual partition assignment includes broker 5, but no such broker is registered."), 
                        new ReassignablePartitionResponse().setPartitionIndex(3).setErrorCode(INVALID_REPLICA_ASSIGNMENT.code()).
                            setErrorMessage("The manual partition assignment includes an empty replica list."))),
                    new ReassignableTopicResponse().setName("bar").setPartitions(singletonList(
                        new ReassignablePartitionResponse().setPartitionIndex(0).setErrorMessage(null))))),
            alterResult.response());
        ctx.replay(alterResult.records());
        assertEquals(new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 4}).setIsr(new int[] {1, 2, 4}).
            setDirectories(new Uuid[] {
                    Uuid.fromString("TESTBROKER00001DIRAAAA"),
                    Uuid.fromString("TESTBROKER00002DIRAAAA"),
                    Uuid.fromString("TESTBROKER00004DIRAAAA")
            }).
            setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(1).setPartitionEpoch(2).build(), replication.getPartition(fooId, 0));
        assertEquals(new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 0}).setIsr(new int[] {0, 1, 2}).
            setDirectories(new Uuid[] {
                    Uuid.fromString("TESTBROKER00001DIRAAAA"),
                    Uuid.fromString("TESTBROKER00002DIRAAAA"),
                    Uuid.fromString("TESTBROKER00003DIRAAAA"),
                    Uuid.fromString("TESTBROKER00000DIRAAAA")
            }).
            setLeader(0).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(0).setPartitionEpoch(2).build(), replication.getPartition(fooId, 1));
        assertEquals(new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4, 0}).setIsr(new int[] {4, 2}).
            setDirectories(new Uuid[] {
                    Uuid.fromString("TESTBROKER00001DIRAAAA"),
                    Uuid.fromString("TESTBROKER00002DIRAAAA"),
                    Uuid.fromString("TESTBROKER00003DIRAAAA"),
                    Uuid.fromString("TESTBROKER00004DIRAAAA"),
                    Uuid.fromString("TESTBROKER00000DIRAAAA")
            }).
            setAddingReplicas(new int[] {0, 1}).setLeader(4).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(0).setPartitionEpoch(2).build(), replication.getPartition(barId, 0));
        ListPartitionReassignmentsResponseData currentReassigning =
            new ListPartitionReassignmentsResponseData().setErrorMessage(null).
                setTopics(singletonList(new OngoingTopicReassignment().
                    setName("bar").setPartitions(singletonList(
                        new OngoingPartitionReassignment().setPartitionIndex(0).
                            setRemovingReplicas(Collections.emptyList()).
                            setAddingReplicas(asList(0, 1)).
                            setReplicas(asList(1, 2, 3, 4, 0))))));
        assertEquals(currentReassigning, replication.listPartitionReassignments(null, Long.MAX_VALUE));
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(singletonList(
            new ListPartitionReassignmentsTopics().setName("foo").
                setPartitionIndexes(asList(0, 1, 2))), Long.MAX_VALUE));
        assertEquals(currentReassigning, replication.listPartitionReassignments(singletonList(
            new ListPartitionReassignmentsTopics().setName("bar").
                setPartitionIndexes(asList(0, 1, 2))), Long.MAX_VALUE));
        ControllerResult<AlterPartitionResponseData> alterPartitionResult = replication.alterPartition(
            anonymousContextFor(ApiKeys.ALTER_PARTITION),
            new AlterPartitionRequestData().setBrokerId(4).setBrokerEpoch(104).
                setTopics(singletonList(new TopicData().setTopicId(barId).setPartitions(singletonList(
                    new PartitionData().setPartitionIndex(0).setPartitionEpoch(2).
                        setLeaderEpoch(0).setNewIsrWithEpochs(isrWithDefaultEpoch(4, 1, 2, 0)))))));
        assertEquals(new AlterPartitionResponseData().setTopics(singletonList(
            new AlterPartitionResponseData.TopicData().setTopicId(barId).setPartitions(singletonList(
                new AlterPartitionResponseData.PartitionData().
                    setPartitionIndex(0).
                    setLeaderId(4).
                    setLeaderEpoch(0).
                    setIsr(asList(4, 1, 2, 0)).
                    setPartitionEpoch(3).
                    setErrorCode(NONE.code()))))),
            alterPartitionResult.response());
        ControllerResult<AlterPartitionReassignmentsResponseData> cancelResult =
            replication.alterPartitionReassignments(
                new AlterPartitionReassignmentsRequestData().setTopics(asList(
                    new ReassignableTopic().setName("foo").setPartitions(singletonList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(null))),
                    new ReassignableTopic().setName("bar").setPartitions(singletonList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(null))))));
        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
                new PartitionChangeRecord().setTopicId(barId).
                    setPartitionId(0).
                    setLeader(4).
                    setReplicas(asList(2, 3, 4)).
                    setDirectories(asList(
                            Uuid.fromString("TESTBROKER00002DIRAAAA"),
                            Uuid.fromString("TESTBROKER00003DIRAAAA"),
                            Uuid.fromString("TESTBROKER00004DIRAAAA")
                    )).
                    setRemovingReplicas(null).
                    setAddingReplicas(Collections.emptyList()), MetadataVersion.latestTesting().partitionChangeRecordVersion())),
            new AlterPartitionReassignmentsResponseData().setErrorMessage(null).setResponses(asList(
                new ReassignableTopicResponse().setName("foo").setPartitions(singletonList(
                    new ReassignablePartitionResponse().setPartitionIndex(0).
                        setErrorCode(NO_REASSIGNMENT_IN_PROGRESS.code()).setErrorMessage(null))),
                new ReassignableTopicResponse().setName("bar").setPartitions(singletonList(
                    new ReassignablePartitionResponse().setPartitionIndex(0).
                        setErrorMessage(null)))))),
            cancelResult);
        ctx.replay(cancelResult.records());
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(null, Long.MAX_VALUE));
        assertEquals(new PartitionRegistration.Builder().setReplicas(new int[] {2, 3, 4}).setIsr(new int[] {4, 2}).
            setDirectories(new Uuid[] {
                    Uuid.fromString("TESTBROKER00002DIRAAAA"),
                    Uuid.fromString("TESTBROKER00003DIRAAAA"),
                    Uuid.fromString("TESTBROKER00004DIRAAAA")
            }).
            setLeader(4).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(1).setPartitionEpoch(3).build(), replication.getPartition(barId, 0));
    }

    @Test
    public void testManualPartitionAssignmentOnAllFencedBrokers() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0, 1, 2, 3);
        ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
            INVALID_REPLICA_ASSIGNMENT.code());
    }

    @Test
    public void testCreatePartitionsFailsWithManualAssignmentWithAllFenced() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0, 1, 2, 3, 4, 5);
        ctx.unfenceBrokers(0, 1, 2);
        Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}}).topicId();
        ctx.createPartitions(2, "foo", new int[][] {new int[] {3, 4, 5}},
            INVALID_REPLICA_ASSIGNMENT.code());
        ctx.createPartitions(2, "foo", new int[][] {new int[] {2, 4, 5}}, NONE.code());
        assertEquals(new PartitionRegistration.Builder().setReplicas(new int[] {2, 4, 5}).
                setDirectories(new Uuid[] {
                        Uuid.fromString("TESTBROKER00002DIRAAAA"),
                        Uuid.fromString("TESTBROKER00004DIRAAAA"),
                        Uuid.fromString("TESTBROKER00005DIRAAAA")
                }).
                setIsr(new int[] {2}).setLeader(2).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(0).setPartitionEpoch(0).build(),
            ctx.replicationControl.getPartition(fooId, 1));
    }

    private void assertLeaderAndIsr(
        ReplicationControlManager replication,
        TopicIdPartition topicIdPartition,
        int leaderId,
        int[] isr
    ) {
        PartitionRegistration registration = replication.getPartition(
            topicIdPartition.topicId(),
            topicIdPartition.partitionId()
        );
        assertArrayEquals(isr, registration.isr);
        assertEquals(leaderId, registration.leader);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testElectUncleanLeaders_WithoutElr(boolean electAllPartitions) {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().
            setMetadataVersion(MetadataVersion.IBP_3_6_IV1).
            build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(0, 1, 2, 3, 4);

        Uuid fooId = ctx.createTestTopic("foo", new int[][]{
            new int[]{1, 2, 3}, new int[]{2, 3, 4}, new int[]{0, 2, 1}}).topicId();

        TopicIdPartition partition0 = new TopicIdPartition(fooId, 0);
        TopicIdPartition partition1 = new TopicIdPartition(fooId, 1);
        TopicIdPartition partition2 = new TopicIdPartition(fooId, 2);

        ctx.fenceBrokers(Set.of(2, 3));
        ctx.fenceBrokers(Set.of(1, 2, 3));

        assertLeaderAndIsr(replication, partition0, NO_LEADER, new int[]{1});
        assertLeaderAndIsr(replication, partition1, 4, new int[]{4});
        assertLeaderAndIsr(replication, partition2, 0, new int[]{0});

        ElectLeadersRequestData request = buildElectLeadersRequest(
            ElectionType.UNCLEAN,
            electAllPartitions ? null : singletonMap("foo", asList(0, 1, 2))
        );

        // No election can be done yet because no replicas are available for partition 0
        ControllerResult<ElectLeadersResponseData> result1 = replication.electLeaders(request);
        assertEquals(Collections.emptyList(), result1.records());

        ElectLeadersResponseData expectedResponse1 = buildElectLeadersResponse(NONE, electAllPartitions, Utils.mkMap(
            Utils.mkEntry(
                new TopicPartition("foo", 0),
                new ApiError(ELIGIBLE_LEADERS_NOT_AVAILABLE)
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 1),
                new ApiError(ELECTION_NOT_NEEDED)
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 2),
                new ApiError(ELECTION_NOT_NEEDED)
            )
        ));
        assertElectLeadersResponse(expectedResponse1, result1.response());

        // Now we bring 2 back online which should allow the unclean election of partition 0
        ctx.unfenceBrokers(2);

        // Bring 2 back into the ISR for partition 1. This allows us to verify that
        // preferred election does not occur as a result of the unclean election request.
        ctx.alterPartition(partition1, 4, isrWithDefaultEpoch(2, 4), LeaderRecoveryState.RECOVERED);

        ControllerResult<ElectLeadersResponseData> result = replication.electLeaders(request);
        assertEquals(1, result.records().size());

        ApiMessageAndVersion record = result.records().get(0);
        assertInstanceOf(PartitionChangeRecord.class, record.message());

        PartitionChangeRecord partitionChangeRecord = (PartitionChangeRecord) record.message();
        assertEquals(0, partitionChangeRecord.partitionId());
        assertEquals(2, partitionChangeRecord.leader());
        assertEquals(singletonList(2), partitionChangeRecord.isr());
        ctx.replay(result.records());

        assertLeaderAndIsr(replication, partition0, 2, new int[]{2});
        assertLeaderAndIsr(replication, partition1, 4, new int[]{2, 4});
        assertLeaderAndIsr(replication, partition2, 0, new int[]{0});

        ElectLeadersResponseData expectedResponse = buildElectLeadersResponse(NONE, electAllPartitions, Utils.mkMap(
            Utils.mkEntry(
                new TopicPartition("foo", 0),
                ApiError.NONE
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 1),
                new ApiError(ELECTION_NOT_NEEDED)
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 2),
                new ApiError(ELECTION_NOT_NEEDED)
            )
        ));
        assertElectLeadersResponse(expectedResponse, result.response());
    }

    @Test
    public void testPreferredElectionDoesNotTriggerUncleanElection() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(1, 2, 3, 4);
        ctx.unfenceBrokers(1, 2, 3, 4);

        Uuid fooId = ctx.createTestTopic("foo", new int[][]{new int[]{1, 2, 3}}).topicId();
        TopicIdPartition partition = new TopicIdPartition(fooId, 0);

        ctx.fenceBrokers(Set.of(2, 3));
        ctx.fenceBrokers(Set.of(1, 2, 3));
        ctx.unfenceBrokers(2);

        assertLeaderAndIsr(replication, partition, NO_LEADER, new int[]{1});

        ctx.alterTopicConfig("foo", "unclean.leader.election.enable", "true");

        ElectLeadersRequestData request = buildElectLeadersRequest(
            ElectionType.PREFERRED,
            singletonMap("foo", singletonList(0))
        );

        // No election should be done even though unclean election is available
        ControllerResult<ElectLeadersResponseData> result = replication.electLeaders(request);
        assertEquals(Collections.emptyList(), result.records());

        ElectLeadersResponseData expectedResponse = buildElectLeadersResponse(NONE, false, singletonMap(
            new TopicPartition("foo", 0), new ApiError(PREFERRED_LEADER_NOT_AVAILABLE)
        ));
        assertEquals(expectedResponse, result.response());
    }

    private ElectLeadersRequestData buildElectLeadersRequest(
        ElectionType electionType,
        Map<String, List<Integer>> partitions
    ) {
        ElectLeadersRequestData request = new ElectLeadersRequestData().
            setElectionType(electionType.value);

        if (partitions == null) {
            request.setTopicPartitions(null);
        } else {
            partitions.forEach((topic, partitionIds) -> {
                request.topicPartitions().add(new TopicPartitions()
                    .setTopic(topic)
                    .setPartitions(partitionIds)
                );
            });
        }
        return request;
    }

    @Test
    public void testFenceMultipleBrokers() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(0, 1, 2, 3, 4);

        Uuid fooId = ctx.createTestTopic("foo", new int[][]{
            new int[]{1, 2, 3}, new int[]{2, 3, 4}, new int[]{0, 2, 1}}).topicId();

        assertTrue(ctx.fencedBrokerIds().isEmpty());
        ctx.fenceBrokers(Set.of(2, 3));

        PartitionRegistration partition0 = replication.getPartition(fooId, 0);
        PartitionRegistration partition1 = replication.getPartition(fooId, 1);
        PartitionRegistration partition2 = replication.getPartition(fooId, 2);

        assertArrayEquals(new int[]{1, 2, 3}, partition0.replicas);
        assertArrayEquals(new int[]{1}, partition0.isr);
        assertEquals(1, partition0.leader);

        assertArrayEquals(new int[]{2, 3, 4}, partition1.replicas);
        assertArrayEquals(new int[]{4}, partition1.isr);
        assertEquals(4, partition1.leader);

        assertArrayEquals(new int[]{0, 2, 1}, partition2.replicas);
        assertArrayEquals(new int[]{0, 1}, partition2.isr);
        assertNotEquals(2, partition2.leader);
    }

    @Test
    public void testElectPreferredLeaders() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(1, 2, 3, 4);
        ctx.inControlledShutdownBrokers(1);
        Uuid fooId = ctx.createTestTopic("foo", new int[][]{
            new int[]{1, 2, 3}, new int[]{2, 3, 4}, new int[]{0, 2, 1}}).topicId();
        ElectLeadersRequestData request1 = new ElectLeadersRequestData().
            setElectionType(ElectionType.PREFERRED.value).
            setTopicPartitions(new TopicPartitionsCollection(asList(
                new TopicPartitions().setTopic("foo").
                    setPartitions(asList(0, 1, 2)),
                new TopicPartitions().setTopic("bar").
                    setPartitions(asList(0, 1))).iterator()));
        ControllerResult<ElectLeadersResponseData> election1Result =
            replication.electLeaders(request1);
        ElectLeadersResponseData expectedResponse1 = buildElectLeadersResponse(NONE, false, Utils.mkMap(
            Utils.mkEntry(
                new TopicPartition("foo", 0),
                new ApiError(PREFERRED_LEADER_NOT_AVAILABLE)
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 1),
                new ApiError(ELECTION_NOT_NEEDED)
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 2),
                new ApiError(PREFERRED_LEADER_NOT_AVAILABLE)
            ),
            Utils.mkEntry(
                new TopicPartition("bar", 0),
                new ApiError(UNKNOWN_TOPIC_OR_PARTITION, "No such topic as bar")
            ),
            Utils.mkEntry(
                new TopicPartition("bar", 1),
                new ApiError(UNKNOWN_TOPIC_OR_PARTITION, "No such topic as bar")
            )
        ));
        assertElectLeadersResponse(expectedResponse1, election1Result.response());
        assertEquals(Collections.emptyList(), election1Result.records());

        // Broker 1 must be registered to get out from the controlled shutdown state.
        ctx.registerBrokers(1);
        ctx.unfenceBrokers(0, 1);

        ControllerResult<AlterPartitionResponseData> alterPartitionResult = replication.alterPartition(
            anonymousContextFor(ApiKeys.ALTER_PARTITION),
            new AlterPartitionRequestData().setBrokerId(2).setBrokerEpoch(102).
                setTopics(singletonList(new TopicData().setTopicId(fooId).
                    setPartitions(asList(
                        new PartitionData().
                            setPartitionIndex(0).setPartitionEpoch(0).
                            setLeaderEpoch(0).setNewIsrWithEpochs(isrWithDefaultEpoch(1, 2, 3)),
                        new PartitionData().
                            setPartitionIndex(2).setPartitionEpoch(0).
                            setLeaderEpoch(0).setNewIsrWithEpochs(isrWithDefaultEpoch(0, 2, 1)))))));
        assertEquals(new AlterPartitionResponseData().setTopics(singletonList(
            new AlterPartitionResponseData.TopicData().setTopicId(fooId).setPartitions(asList(
                new AlterPartitionResponseData.PartitionData().
                    setPartitionIndex(0).
                    setLeaderId(2).
                    setLeaderEpoch(0).
                    setIsr(asList(1, 2, 3)).
                    setPartitionEpoch(1).
                    setErrorCode(NONE.code()),
                new AlterPartitionResponseData.PartitionData().
                    setPartitionIndex(2).
                    setLeaderId(2).
                    setLeaderEpoch(0).
                    setIsr(asList(0, 2, 1)).
                    setPartitionEpoch(1).
                    setErrorCode(NONE.code()))))),
            alterPartitionResult.response());

        ElectLeadersResponseData expectedResponse2 = buildElectLeadersResponse(NONE, false, Utils.mkMap(
            Utils.mkEntry(
                new TopicPartition("foo", 0),
                ApiError.NONE
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 1),
                new ApiError(ELECTION_NOT_NEEDED)
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 2),
                ApiError.NONE
            ),
            Utils.mkEntry(
                new TopicPartition("bar", 0),
                new ApiError(UNKNOWN_TOPIC_OR_PARTITION, "No such topic as bar")
            ),
            Utils.mkEntry(
                new TopicPartition("bar", 1),
                new ApiError(UNKNOWN_TOPIC_OR_PARTITION, "No such topic as bar")
            )
        ));

        ctx.replay(alterPartitionResult.records());
        ControllerResult<ElectLeadersResponseData> election2Result =
            replication.electLeaders(request1);
        assertElectLeadersResponse(expectedResponse2, election2Result.response());
        assertEquals(
            asList(
                new ApiMessageAndVersion(
                    new PartitionChangeRecord().
                        setPartitionId(0).
                        setTopicId(fooId).
                        setLeader(1),
                    MetadataVersion.latestTesting().partitionChangeRecordVersion()),
                new ApiMessageAndVersion(
                    new PartitionChangeRecord().
                        setPartitionId(2).
                        setTopicId(fooId).
                        setLeader(0),
                    MetadataVersion.latestTesting().partitionChangeRecordVersion())),
            election2Result.records());
    }

    @Test
    public void testBalancePartitionLeaders() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(2, 3, 4);
        Uuid fooId = ctx.createTestTopic("foo", new int[][]{
            new int[]{1, 2, 3}, new int[]{2, 3, 4}, new int[]{0, 2, 1}}).topicId();

        assertTrue(replication.arePartitionLeadersImbalanced());

        ctx.unfenceBrokers(1);

        ControllerResult<AlterPartitionResponseData> alterPartitionResult = replication.alterPartition(
            anonymousContextFor(ApiKeys.ALTER_PARTITION),
            new AlterPartitionRequestData().setBrokerId(2).setBrokerEpoch(102).
                setTopics(singletonList(new TopicData().setTopicId(fooId).
                    setPartitions(singletonList(new PartitionData().
                        setPartitionIndex(0).setPartitionEpoch(0).
                        setLeaderEpoch(0).setNewIsrWithEpochs(isrWithDefaultEpoch(1, 2, 3)))))));
        assertEquals(new AlterPartitionResponseData().setTopics(singletonList(
            new AlterPartitionResponseData.TopicData().setTopicId(fooId).setPartitions(singletonList(
                new AlterPartitionResponseData.PartitionData().
                    setPartitionIndex(0).
                    setLeaderId(2).
                    setLeaderEpoch(0).
                    setIsr(asList(1, 2, 3)).
                    setPartitionEpoch(1).
                    setErrorCode(NONE.code()))))),
            alterPartitionResult.response());
        ctx.replay(alterPartitionResult.records());

        ControllerResult<Boolean> balanceResult = replication.maybeBalancePartitionLeaders();
        ctx.replay(balanceResult.records());

        PartitionChangeRecord expectedChangeRecord = new PartitionChangeRecord()
            .setPartitionId(0)
            .setTopicId(fooId)
            .setLeader(1);
        assertEquals(singletonList(new ApiMessageAndVersion(expectedChangeRecord, MetadataVersion.latestTesting().partitionChangeRecordVersion())), balanceResult.records());
        assertTrue(replication.arePartitionLeadersImbalanced());
        assertFalse(balanceResult.response());

        ctx.unfenceBrokers(0);

        alterPartitionResult = replication.alterPartition(
            anonymousContextFor(ApiKeys.ALTER_PARTITION),
            new AlterPartitionRequestData().setBrokerId(2).setBrokerEpoch(102).
                setTopics(singletonList(new TopicData().setTopicId(fooId).
                    setPartitions(singletonList(new PartitionData().
                        setPartitionIndex(2).setPartitionEpoch(0).
                        setLeaderEpoch(0).setNewIsrWithEpochs(isrWithDefaultEpoch(0, 2, 1)))))));
        assertEquals(new AlterPartitionResponseData().setTopics(singletonList(
            new AlterPartitionResponseData.TopicData().setTopicId(fooId).setPartitions(singletonList(
                new AlterPartitionResponseData.PartitionData().
                    setPartitionIndex(2).
                    setLeaderId(2).
                    setLeaderEpoch(0).
                    setIsr(asList(0, 2, 1)).
                    setPartitionEpoch(1).
                    setErrorCode(NONE.code()))))),
            alterPartitionResult.response());
        ctx.replay(alterPartitionResult.records());

        balanceResult = replication.maybeBalancePartitionLeaders();
        ctx.replay(balanceResult.records());

        expectedChangeRecord = new PartitionChangeRecord()
            .setPartitionId(2)
            .setTopicId(fooId)
            .setLeader(0);
        assertEquals(singletonList(new ApiMessageAndVersion(expectedChangeRecord, MetadataVersion.latestTesting().partitionChangeRecordVersion())), balanceResult.records());
        assertFalse(replication.arePartitionLeadersImbalanced());
        assertFalse(balanceResult.response());
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "static", "dynamic_cluster", "dynamic_node", "dynamic_topic"})
    public void testMaybeTriggerUncleanLeaderElectionForLeaderlessPartitions(String uncleanConfig) {
        ReplicationControlTestContext.Builder ctxBuilder = new ReplicationControlTestContext.Builder();
        if (uncleanConfig.equals("static")) {
            ctxBuilder.setStaticConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true");
        }
        ReplicationControlTestContext ctx = ctxBuilder.build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(0, 1, 2, 3, 4);
        Uuid fooId = ctx.createTestTopic("foo", new int[][]{
            new int[]{1, 2, 4}, new int[]{1, 3, 4}, new int[]{0, 2, 4}}).topicId();
        assertFalse(replication.areSomePartitionsLeaderless());
        ctx.fenceBrokers(0, 1, 2, 3, 4);
        assertTrue(replication.areSomePartitionsLeaderless());
        for (int partitionId : Arrays.asList(0, 1, 2)) {
            assertArrayEquals(new int[] {4}, ctx.replicationControl.getPartition(fooId, partitionId).isr);
            assertEquals(-1, ctx.replicationControl.getPartition(fooId, partitionId).leader);
        }

        // Unfence broker 2. It is now available to be the leader for partition 0 and 2, after
        // an unclean election.
        ctx.unfenceBrokers(2);

        if (uncleanConfig.equals("static")) {
            // If we statically configured unclean leader election, the election already happened.
            assertArrayEquals(new int[] {2}, ctx.replicationControl.getPartition(fooId, 0).isr);
            assertEquals(2, ctx.replicationControl.getPartition(fooId, 0).leader);
            assertArrayEquals(new int[] {4}, ctx.replicationControl.getPartition(fooId, 1).isr);
            assertEquals(-1, ctx.replicationControl.getPartition(fooId, 1).leader);
            assertArrayEquals(new int[] {2}, ctx.replicationControl.getPartition(fooId, 2).isr);
            assertEquals(2, ctx.replicationControl.getPartition(fooId, 2).leader);
        } else {
            // Otherwise, check that the election did NOT happen.
            for (int partitionId : Arrays.asList(0, 1, 2)) {
                assertArrayEquals(new int[] {4}, ctx.replicationControl.getPartition(fooId, partitionId).isr);
                assertEquals(-1, ctx.replicationControl.getPartition(fooId, partitionId).leader);
            }
        }

        // If we're setting unclean leader election dynamically, do that here.
        if (uncleanConfig.equals("dynamic_cluster")) {
            ctx.replay(ctx.configurationControl.incrementalAlterConfigs(
                Collections.singletonMap(new ConfigResource(ConfigResource.Type.BROKER, ""),
                    Collections.singletonMap(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
                        new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true"))),
                true).records());
        } else if (uncleanConfig.equals("dynamic_node")) {
            ctx.replay(ctx.configurationControl.incrementalAlterConfigs(
                Collections.singletonMap(new ConfigResource(ConfigResource.Type.BROKER, "0"),
                    Collections.singletonMap(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
                        new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true"))),
                true).records());
        } else if (uncleanConfig.equals("dynamic_topic")) {
            ctx.replay(ctx.configurationControl.incrementalAlterConfigs(
                Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, "foo"),
                    Collections.singletonMap(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
                        new AbstractMap.SimpleImmutableEntry<>(AlterConfigOp.OpType.SET, "true"))),
                true).records());
        }
        ControllerResult<Boolean> balanceResult = replication.maybeElectUncleanLeaders();
        assertFalse(balanceResult.response());
        if (uncleanConfig.equals("none") || uncleanConfig.equals("static")) {
            assertEquals(0, balanceResult.records().size(), "Expected no records, but " +
                balanceResult.records().size() + " were found.");
        } else {
            assertNotEquals(0, balanceResult.records().size(), "Expected some records, but " +
                "none were found.");
            ctx.replay(balanceResult.records());
            assertArrayEquals(new int[] {2}, ctx.replicationControl.getPartition(fooId, 0).isr);
            assertEquals(2, ctx.replicationControl.getPartition(fooId, 0).leader);
            assertArrayEquals(new int[] {4}, ctx.replicationControl.getPartition(fooId, 1).isr);
            assertEquals(-1, ctx.replicationControl.getPartition(fooId, 1).leader);
            assertArrayEquals(new int[] {2}, ctx.replicationControl.getPartition(fooId, 2).isr);
            assertEquals(2, ctx.replicationControl.getPartition(fooId, 2).leader);
        }
    }

    private void assertElectLeadersResponse(
        ElectLeadersResponseData expected,
        ElectLeadersResponseData actual
    ) {
        assertEquals(Errors.forCode(expected.errorCode()), Errors.forCode(actual.errorCode()));
        assertEquals(collectElectLeadersErrors(expected), collectElectLeadersErrors(actual));
    }

    private Map<TopicPartition, PartitionResult> collectElectLeadersErrors(ElectLeadersResponseData response) {
        Map<TopicPartition, PartitionResult> res = new HashMap<>();
        response.replicaElectionResults().forEach(topicResult -> {
            String topic = topicResult.topic();
            topicResult.partitionResult().forEach(partitionResult -> {
                TopicPartition topicPartition = new TopicPartition(topic, partitionResult.partitionId());
                res.put(topicPartition, partitionResult);
            });
        });
        return res;
    }

    private ElectLeadersResponseData buildElectLeadersResponse(
        Errors topLevelError,
        boolean electAllPartitions,
        Map<TopicPartition, ApiError> errors
    ) {
        Map<String, List<Map.Entry<TopicPartition, ApiError>>> errorsByTopic = errors.entrySet().stream()
            .collect(Collectors.groupingBy(entry -> entry.getKey().topic()));

        ElectLeadersResponseData response = new ElectLeadersResponseData()
            .setErrorCode(topLevelError.code());

        errorsByTopic.forEach((topic, partitionErrors) -> {
            ReplicaElectionResult electionResult = new ReplicaElectionResult().setTopic(topic);
            electionResult.setPartitionResult(partitionErrors.stream()
                .filter(entry -> !electAllPartitions || entry.getValue().error() != ELECTION_NOT_NEEDED)
                .map(entry -> {
                    TopicPartition topicPartition = entry.getKey();
                    ApiError error = entry.getValue();
                    return new PartitionResult()
                        .setPartitionId(topicPartition.partition())
                        .setErrorCode(error.error().code())
                        .setErrorMessage(error.message());
                })
                .collect(Collectors.toList()));
            response.replicaElectionResults().add(electionResult);
        });

        return response;
    }

    @Test
    public void testKRaftClusterDescriber() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokersWithDirs(
                0, Collections.emptyList(),
                1, Collections.emptyList(),
                2, asList(Uuid.fromString("ozwqsVMFSNiYQUPSJA3j0w")),
                3, asList(Uuid.fromString("SSDgCZ4BTyec5QojGT65qg"), Uuid.fromString("K8KwMrviRcOUvgI8FPOJWg")),
                4, Collections.emptyList()
        );
        ctx.unfenceBrokers(2, 3, 4);
        ctx.createTestTopic("foo", new int[][]{
            new int[]{1, 2, 3}, new int[]{2, 3, 4}, new int[]{0, 2, 1}}).topicId();
        ctx.createTestTopic("bar", new int[][]{
            new int[]{2, 3, 4}, new int[]{3, 4, 2}}).topicId();
        KRaftClusterDescriber describer = replication.clusterDescriber;
        HashSet<UsableBroker> brokers = new HashSet<>();
        describer.usableBrokers().forEachRemaining(broker -> brokers.add(broker));
        assertEquals(new HashSet<>(Arrays.asList(
            new UsableBroker(0, Optional.empty(), true),
            new UsableBroker(1, Optional.empty(), true),
            new UsableBroker(2, Optional.empty(), false),
            new UsableBroker(3, Optional.empty(), false),
            new UsableBroker(4, Optional.empty(), false))), brokers);
        assertEquals(DirectoryId.MIGRATING, describer.defaultDir(1));
        assertEquals(Uuid.fromString("ozwqsVMFSNiYQUPSJA3j0w"), describer.defaultDir(2));
        assertEquals(DirectoryId.UNASSIGNED, describer.defaultDir(3));
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class, names = {"IBP_3_3_IV2", "IBP_3_3_IV3"})
    public void testProcessBrokerHeartbeatInControlledShutdown(MetadataVersion metadataVersion) {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().
                setMetadataVersion(metadataVersion).
                build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);

        Uuid topicId = ctx.createTestTopic("foo", new int[][]{new int[]{0, 1, 2}}).topicId();

        BrokerHeartbeatRequestData heartbeatRequest = new BrokerHeartbeatRequestData()
            .setBrokerId(0)
            .setBrokerEpoch(100)
            .setCurrentMetadataOffset(0)
            .setWantShutDown(true);

        ControllerResult<BrokerHeartbeatReply> result = ctx.replicationControl
            .processBrokerHeartbeat(heartbeatRequest, 0);

        List<ApiMessageAndVersion> expectedRecords = new ArrayList<>();

        if (metadataVersion.isInControlledShutdownStateSupported()) {
            expectedRecords.add(new ApiMessageAndVersion(
                new BrokerRegistrationChangeRecord()
                    .setBrokerEpoch(100)
                    .setBrokerId(0)
                    .setInControlledShutdown(BrokerRegistrationInControlledShutdownChange
                        .IN_CONTROLLED_SHUTDOWN.value()),
                (short) 1));
        }

        expectedRecords.add(new ApiMessageAndVersion(
            new PartitionChangeRecord()
                .setPartitionId(0)
                .setTopicId(topicId)
                .setIsr(asList(1, 2))
                .setLeader(1),
            (short) 0));

        assertEquals(expectedRecords, result.records());
    }

    @Test
    public void testProcessExpiredBrokerHeartbeat() {
        MockTime mockTime = new MockTime(0, 0, 0);
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().
                setMockTime(mockTime).
                build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);

        BrokerHeartbeatRequestData heartbeatRequest = new BrokerHeartbeatRequestData().
                setBrokerId(0).
                setBrokerEpoch(100).
                setCurrentMetadataOffset(123).
                setWantShutDown(false);
        mockTime.sleep(100);
        ctx.replicationControl.processExpiredBrokerHeartbeat(heartbeatRequest);
        Optional<BrokerHeartbeatState> state =
            ctx.clusterControl.heartbeatManager().brokers().stream().
                filter(broker -> broker.id() == 0).findFirst();
        assertTrue(state.isPresent());
        assertEquals(0, state.get().id());
        assertEquals(123, state.get().metadataOffset());
    }

    @Test
    public void testReassignPartitionsHandlesNewReassignmentThatRemovesPreviouslyAddingReplicas() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4, 5);
        ctx.unfenceBrokers(0, 1, 2, 3, 4, 5);

        String topic = "topic-1";
        // Create topic with assignment [0, 1]
        Uuid topicId = ctx.createTestTopic(topic, new int[][] {new int[] {0, 1}}).topicId();
        log.debug("Created topic with ID {}", topicId);

        // Confirm we start off with no reassignments.
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(null, Long.MAX_VALUE));

        // Reassign to [2, 3]
        ControllerResult<AlterPartitionReassignmentsResponseData> alterResultOne =
            replication.alterPartitionReassignments(
                new AlterPartitionReassignmentsRequestData().setTopics(singletonList(
                    new ReassignableTopic().setName(topic).setPartitions(singletonList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(asList(2, 3)))))));
        assertEquals(new AlterPartitionReassignmentsResponseData().
            setErrorMessage(null).setResponses(singletonList(
                new ReassignableTopicResponse().setName(topic).setPartitions(singletonList(
                    new ReassignablePartitionResponse().setPartitionIndex(0).
                        setErrorMessage(null))))), alterResultOne.response());
        ctx.replay(alterResultOne.records());

        ListPartitionReassignmentsResponseData currentReassigning =
            new ListPartitionReassignmentsResponseData().setErrorMessage(null).
                setTopics(singletonList(new OngoingTopicReassignment().
                    setName(topic).setPartitions(singletonList(
                        new OngoingPartitionReassignment().setPartitionIndex(0).
                            setRemovingReplicas(asList(0, 1)).
                            setAddingReplicas(asList(2, 3)).
                            setReplicas(asList(2, 3, 0, 1))))));

        // Make sure the reassignment metadata is as expected.
        assertEquals(currentReassigning, replication.listPartitionReassignments(null, Long.MAX_VALUE));

        PartitionRegistration partition = replication.getPartition(topicId, 0);

        // Add replica 2 to the ISR.
        AlterPartitionRequestData alterPartitionRequestData = new AlterPartitionRequestData().
            setBrokerId(partition.leader).
            setBrokerEpoch(ctx.currentBrokerEpoch(partition.leader)).
            setTopics(singletonList(new TopicData().
                setTopicId(topicId).
                setPartitions(singletonList(new PartitionData().
                    setPartitionIndex(0).
                    setPartitionEpoch(partition.partitionEpoch).
                    setLeaderEpoch(partition.leaderEpoch).
                    setNewIsrWithEpochs(isrWithDefaultEpoch(0, 1, 2))))));
        ControllerResult<AlterPartitionResponseData> alterPartitionResult = replication.alterPartition(
            anonymousContextFor(ApiKeys.ALTER_PARTITION),
            new AlterPartitionRequest.Builder(alterPartitionRequestData, true).build().data());
        assertEquals(new AlterPartitionResponseData().setTopics(singletonList(
            new AlterPartitionResponseData.TopicData().
                setTopicId(topicId).
                setPartitions(singletonList(
                    new AlterPartitionResponseData.PartitionData().
                        setPartitionIndex(0).
                        setIsr(asList(0, 1, 2)).
                        setPartitionEpoch(partition.partitionEpoch + 1).
                        setErrorCode(NONE.code()))))),
            alterPartitionResult.response());

        ctx.replay(alterPartitionResult.records());

        // Elect replica 2 as leader via preferred leader election. 2 is at the front of the replicas list.
        ElectLeadersRequestData request = buildElectLeadersRequest(
            ElectionType.PREFERRED,
            singletonMap(topic, singletonList(0))
        );
        ControllerResult<ElectLeadersResponseData> electLeaderTwoResult = replication.electLeaders(request);
        ReplicaElectionResult replicaElectionResult = new ReplicaElectionResult().setTopic(topic);
        replicaElectionResult.setPartitionResult(singletonList(new PartitionResult().setPartitionId(0).setErrorCode(NONE.code()).setErrorMessage(null)));
        assertEquals(
            new ElectLeadersResponseData().setErrorCode(NONE.code()).setReplicaElectionResults(singletonList(replicaElectionResult)),
            electLeaderTwoResult.response()
        );
        ctx.replay(electLeaderTwoResult.records());
        // Make sure 2 is the leader
        partition = replication.getPartition(topicId, 0);
        assertEquals(2, partition.leader);

        // Reassign to [4, 5]
        ControllerResult<AlterPartitionReassignmentsResponseData> alterResultTwo =
            replication.alterPartitionReassignments(
                new AlterPartitionReassignmentsRequestData().setTopics(singletonList(
                    new ReassignableTopic().setName(topic).setPartitions(singletonList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(asList(4, 5)))))));
        assertEquals(new AlterPartitionReassignmentsResponseData().
            setErrorMessage(null).setResponses(singletonList(
                new ReassignableTopicResponse().setName(topic).setPartitions(singletonList(
                    new ReassignablePartitionResponse().setPartitionIndex(0).
                        setErrorMessage(null))))), alterResultTwo.response());
        ctx.replay(alterResultTwo.records());

        // Make sure the replicas list contains all the previous replicas 0, 1, 2, 3 as well as the new replicas 3, 4
        currentReassigning =
            new ListPartitionReassignmentsResponseData().setErrorMessage(null).
                setTopics(singletonList(new OngoingTopicReassignment().
                    setName(topic).setPartitions(singletonList(
                        new OngoingPartitionReassignment().setPartitionIndex(0).
                            setRemovingReplicas(asList(0, 1, 2, 3)).
                            setAddingReplicas(asList(4, 5)).
                            setReplicas(asList(4, 5, 0, 1, 2, 3))))));

        assertEquals(currentReassigning, replication.listPartitionReassignments(null, Long.MAX_VALUE));

        // Make sure the leader is in the replicas still
        partition = replication.getPartition(topicId, 0);
        assertEquals(2, partition.leader);
        assertTrue(Replicas.toSet(partition.replicas).contains(partition.leader));

        // Add 3, 4 to the ISR to complete the reassignment
        AlterPartitionRequestData alterPartitionRequestDataTwo = new AlterPartitionRequestData().
            setBrokerId(partition.leader).
            setBrokerEpoch(ctx.currentBrokerEpoch(partition.leader)).
            setTopics(singletonList(new TopicData().
                setTopicId(topicId).
                setPartitions(singletonList(new PartitionData().
                    setPartitionIndex(0).
                    setPartitionEpoch(partition.partitionEpoch).
                    setLeaderEpoch(partition.leaderEpoch).
                    setNewIsrWithEpochs(isrWithDefaultEpoch(0, 1, 2, 3, 4, 5))))));
        ControllerResult<AlterPartitionResponseData> alterPartitionResultTwo = replication.alterPartition(
            anonymousContextFor(ApiKeys.ALTER_PARTITION),
            new AlterPartitionRequest.Builder(alterPartitionRequestDataTwo, true).build().data());
        assertEquals(new AlterPartitionResponseData().setTopics(singletonList(
                new AlterPartitionResponseData.TopicData().
                    setTopicId(topicId).
                    setPartitions(singletonList(
                        new AlterPartitionResponseData.PartitionData().
                            setPartitionIndex(0).
                            setErrorCode(NEW_LEADER_ELECTED.code()))))),
            alterPartitionResultTwo.response());
        ctx.replay(alterPartitionResultTwo.records());

        // After reassignment is finally complete, make sure 4 is the leader now.
        partition = replication.getPartition(topicId, 0);
        assertEquals(4, partition.leader);
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(null, Long.MAX_VALUE));
    }

    private static BrokerState brokerState(int brokerId, Long brokerEpoch) {
        return new BrokerState().setBrokerId(brokerId).setBrokerEpoch(brokerEpoch);
    }

    private static Long defaultBrokerEpoch(int brokerId) {
        return brokerId + 100L;
    }

    private static List<BrokerState> isrWithDefaultEpoch(Integer... isr) {
        return Arrays.stream(isr).map(brokerId -> brokerState(brokerId, defaultBrokerEpoch(brokerId)))
            .collect(Collectors.toList());
    }

    @Test
    public void testDuplicateTopicIdReplay() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        replicationControl.replay(new TopicRecord().
                setName("foo").
                setTopicId(Uuid.fromString("Ktv3YkMQRe-MId4VkkrMyw")));
        assertEquals("Found duplicate TopicRecord for foo with topic ID Ktv3YkMQRe-MId4VkkrMyw",
            assertThrows(RuntimeException.class,
                () -> replicationControl.replay(new TopicRecord().
                    setName("foo").
                    setTopicId(Uuid.fromString("Ktv3YkMQRe-MId4VkkrMyw")))).
                        getMessage());
        assertEquals("Found duplicate TopicRecord for foo with a different ID than before. " +
            "Previous ID was Ktv3YkMQRe-MId4VkkrMyw and new ID is 8auUWq8zQqe_99H_m2LAmw",
                assertThrows(RuntimeException.class,
                        () -> replicationControl.replay(new TopicRecord().
                                setName("foo").
                                setTopicId(Uuid.fromString("8auUWq8zQqe_99H_m2LAmw")))).
                        getMessage());
    }

    @Test
    void testHandleAssignReplicasToDirsFailsOnOlderMv() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().
            setMetadataVersion(MetadataVersion.IBP_3_7_IV1).
            build();
        assertThrows(UnsupportedVersionException.class,
            () -> ctx.replicationControl.handleAssignReplicasToDirs(new AssignReplicasToDirsRequestData()));
    }

    @Test
    void testHandleAssignReplicasToDirs() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        Uuid dir1b1 = Uuid.fromString("hO2YI5bgRUmByNPHiHxjNQ");
        Uuid dir2b1 = Uuid.fromString("R3Gb1HLoTzuKMgAkH5Vtpw");
        Uuid dir1b2 = Uuid.fromString("TBGa8UayQi6KguqF5nC0sw");
        Uuid offlineDir = Uuid.fromString("zvAf9BKZRyyrEWz4FX2nLA");
        ctx.registerBrokersWithDirs(1, asList(dir1b1, dir2b1), 2, singletonList(dir1b2));
        ctx.unfenceBrokers(1, 2);
        Uuid topicA = ctx.createTestTopic("a", new int[][]{new int[]{1, 2}, new int[]{1, 2}, new int[]{1, 2}}).topicId();
        Uuid topicB = ctx.createTestTopic("b", new int[][]{new int[]{1, 2}, new int[]{1, 2}}).topicId();
        Uuid topicC = ctx.createTestTopic("c", new int[][]{new int[]{2}}).topicId();

        ControllerResult<AssignReplicasToDirsResponseData> controllerResult = ctx.assignReplicasToDirs(1, new HashMap<TopicIdPartition, Uuid>() {{
                put(new TopicIdPartition(topicA, 0), dir1b1);
                put(new TopicIdPartition(topicA, 1), dir2b1);
                put(new TopicIdPartition(topicA, 2), offlineDir); // unknown/offline dir
                put(new TopicIdPartition(topicB, 0), dir1b1);
                put(new TopicIdPartition(topicB, 1), DirectoryId.LOST);
                put(new TopicIdPartition(Uuid.fromString("nLU9hKNXSZuMe5PO2A4dVQ"), 1), dir2b1); // expect UNKNOWN_TOPIC_ID
                put(new TopicIdPartition(topicA, 137), dir1b1); // expect UNKNOWN_TOPIC_OR_PARTITION
                put(new TopicIdPartition(topicC, 0), dir1b1); // expect NOT_LEADER_OR_FOLLOWER
            }});

        assertEquals(AssignmentsHelper.normalize(AssignmentsHelper.buildResponseData((short) 0, 0, new HashMap<Uuid, Map<TopicIdPartition, Errors>>() {{
                put(dir1b1, new HashMap<TopicIdPartition, Errors>() {{
                        put(new TopicIdPartition(topicA, 0), NONE);
                        put(new TopicIdPartition(topicA, 137), UNKNOWN_TOPIC_OR_PARTITION);
                        put(new TopicIdPartition(topicB, 0), NONE);
                        put(new TopicIdPartition(topicC, 0), NOT_LEADER_OR_FOLLOWER);
                    }});
                put(dir2b1, new HashMap<TopicIdPartition, Errors>() {{
                        put(new TopicIdPartition(topicA, 1), NONE);
                        put(new TopicIdPartition(Uuid.fromString("nLU9hKNXSZuMe5PO2A4dVQ"), 1), UNKNOWN_TOPIC_ID);
                    }});
                put(offlineDir, new HashMap<TopicIdPartition, Errors>() {{
                        put(new TopicIdPartition(topicA, 2), NONE);
                    }});
                put(DirectoryId.LOST, new HashMap<TopicIdPartition, Errors>() {{
                        put(new TopicIdPartition(topicB, 1), NONE);
                    }});
            }})), AssignmentsHelper.normalize(controllerResult.response()));
        short recordVersion = ctx.featureControl.metadataVersion().partitionChangeRecordVersion();
        assertEquals(sortPartitionChangeRecords(asList(
                new ApiMessageAndVersion(
                        new PartitionChangeRecord().setTopicId(topicA).setPartitionId(0)
                                .setDirectories(asList(dir1b1, dir1b2)), recordVersion),
                new ApiMessageAndVersion(
                        new PartitionChangeRecord().setTopicId(topicA).setPartitionId(1).
                                setDirectories(asList(dir2b1, dir1b2)), recordVersion),
                new ApiMessageAndVersion(
                        new PartitionChangeRecord().setTopicId(topicA).setPartitionId(2).
                                setDirectories(asList(offlineDir, dir1b2)), recordVersion),
                new ApiMessageAndVersion(
                        new PartitionChangeRecord().setTopicId(topicB).setPartitionId(0).
                                setDirectories(asList(dir1b1, dir1b2)), recordVersion),
                new ApiMessageAndVersion(
                        new PartitionChangeRecord().setTopicId(topicB).setPartitionId(1).
                                setDirectories(asList(DirectoryId.LOST, dir1b2)), recordVersion),

                // In addition to the directory assignment changes we expect two additional records,
                // which elect new leaders for:
                //   - a-2 which has been assigned to a directory which is not an online directory (unknown/offline)
                //   - b-1 which has been assigned to an offline directory.
                new ApiMessageAndVersion(
                        new PartitionChangeRecord().setTopicId(topicA).setPartitionId(2).
                                setIsr(singletonList(2)).setLeader(2), recordVersion),
                new ApiMessageAndVersion(
                        new PartitionChangeRecord().setTopicId(topicB).setPartitionId(1).
                                setIsr(singletonList(2)).setLeader(2), recordVersion)
        )), sortPartitionChangeRecords(controllerResult.records()));

        ctx.replay(controllerResult.records());
        assertEquals(new HashSet<TopicIdPartition>() {{
                add(new TopicIdPartition(topicA, 0));
                add(new TopicIdPartition(topicA, 1));
                add(new TopicIdPartition(topicB, 0));
            }}, RecordTestUtils.iteratorToSet(ctx.replicationControl.brokersToIsrs().iterator(1, true)));
        assertEquals(new HashSet<TopicIdPartition>() {{
                add(new TopicIdPartition(topicA, 2));
                add(new TopicIdPartition(topicB, 1));
                add(new TopicIdPartition(topicC, 0));
            }},
            RecordTestUtils.iteratorToSet(ctx.replicationControl.brokersToIsrs().iterator(2, true)));
    }

    @Test
    void testHandleDirectoriesOffline() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        int b1 = 101, b2 = 102;
        Uuid dir1b1 = Uuid.fromString("suitdzfTTdqoWcy8VqmkUg");
        Uuid dir2b1 = Uuid.fromString("yh3acnzGSeurSTj8aIhOjw");
        Uuid dir1b2 = Uuid.fromString("OmpmJ8RjQliQlEFht56DwQ");
        Uuid dir2b2 = Uuid.fromString("w05baLpsT5Oz0LvKTKXoDw");
        ctx.registerBrokersWithDirs(b1, asList(dir1b1, dir2b1), b2, asList(dir1b2, dir2b2));
        ctx.unfenceBrokers(b1, b2);
        Uuid topicA = ctx.createTestTopic("a", new int[][]{new int[]{b1, b2}, new int[]{b1, b2}}).topicId();
        Uuid topicB = ctx.createTestTopic("b", new int[][]{new int[]{b1, b2}, new int[]{b1, b2}}).topicId();
        ctx.assignReplicasToDirs(b1, new HashMap<TopicIdPartition, Uuid>() {{
                put(new TopicIdPartition(topicA, 0), dir1b1);
                put(new TopicIdPartition(topicA, 1), dir2b1);
                put(new TopicIdPartition(topicB, 0), dir1b1);
                put(new TopicIdPartition(topicB, 1), dir2b1);
            }});
        ctx.assignReplicasToDirs(b2, new HashMap<TopicIdPartition, Uuid>() {{
                put(new TopicIdPartition(topicA, 0), dir1b2);
                put(new TopicIdPartition(topicA, 1), dir2b2);
                put(new TopicIdPartition(topicB, 0), dir1b2);
                put(new TopicIdPartition(topicB, 1), dir2b2);
            }});
        List<ApiMessageAndVersion> records = new ArrayList<>();
        ctx.replicationControl.handleDirectoriesOffline(b1, defaultBrokerEpoch(b1), asList(
                dir1b1,
                dir1b2 // should not cause update to dir1b2 as it's not registered to b1
        ), records);
        assertEquals(
            singletonList(new ApiMessageAndVersion(new BrokerRegistrationChangeRecord()
                    .setBrokerId(b1).setBrokerEpoch(defaultBrokerEpoch(b1))
                    .setLogDirs(singletonList(dir2b1)), (short) 2)),
            filter(records, BrokerRegistrationChangeRecord.class)
        );
        short partitionChangeRecordVersion = ctx.featureControl.metadataVersion().partitionChangeRecordVersion();
        assertEquals(
            sortPartitionChangeRecords(asList(
                new ApiMessageAndVersion(new PartitionChangeRecord().setTopicId(topicA).setPartitionId(0)
                        .setLeader(b2).setIsr(singletonList(b2)), partitionChangeRecordVersion),
                new ApiMessageAndVersion(new PartitionChangeRecord().setTopicId(topicB).setPartitionId(0)
                        .setLeader(b2).setIsr(singletonList(b2)), partitionChangeRecordVersion)
            )),
            sortPartitionChangeRecords(filter(records, PartitionChangeRecord.class))
        );
        assertEquals(3, records.size());
        ctx.replay(records);
        assertEquals(Collections.singletonList(dir2b1), ctx.clusterControl.registration(b1).directories());
    }

    /**
     * Sorts {@link PartitionChangeRecord} by topic ID and partition ID,
     * so that the order of the records is deterministic, and can be compared.
     */
    private static List<ApiMessageAndVersion> sortPartitionChangeRecords(List<ApiMessageAndVersion> records) {
        records = new ArrayList<>(records);
        records.sort(Comparator.comparing((ApiMessageAndVersion record) -> {
            PartitionChangeRecord partitionChangeRecord = (PartitionChangeRecord) record.message();
            return partitionChangeRecord.topicId() + "-" + partitionChangeRecord.partitionId();
        }));
        return records;
    }

    private static List<ApiMessageAndVersion> filter(List<ApiMessageAndVersion> records, Class<? extends ApiMessage> clazz) {
        return records.stream().filter(r -> clazz.equals(r.message().getClass())).collect(Collectors.toList());
    }
}
