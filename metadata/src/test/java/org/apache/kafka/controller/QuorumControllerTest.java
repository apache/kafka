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
import org.apache.kafka.common.errors.BrokerIdNotRegisteredException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.Listener;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.ListenerCollection;
import org.apache.kafka.common.message.ControllerRegistrationRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignmentCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.metadata.AbortTransactionRecord;
import org.apache.kafka.common.metadata.BeginTransactionRecord;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.EndTransactionRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpointCollection;
import org.apache.kafka.common.metadata.RegisterControllerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.controller.QuorumController.ConfigResourceExistenceChecker;
import org.apache.kafka.image.AclsDelta;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.image.ClientQuotasDelta;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ClusterDelta;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.ConfigurationsDelta;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.DelegationTokenDelta;
import org.apache.kafka.image.DelegationTokenImage;
import org.apache.kafka.image.FeaturesDelta;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.ProducerIdsDelta;
import org.apache.kafka.image.ProducerIdsImage;
import org.apache.kafka.image.ScramDelta;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.RecordTestUtils.ImageDeltaPair;
import org.apache.kafka.metadata.RecordTestUtils.TestThroughAllIntermediateImagesLeadingToFinalImageHelper;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.util.BatchFileWriter;
import org.apache.kafka.metalog.LocalLogManager;
import org.apache.kafka.metalog.LocalLogManagerTestEnv;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.snapshot.FileRawSnapshotReader;
import org.apache.kafka.snapshot.Snapshots;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.BROKER0;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.entry;
import static org.apache.kafka.controller.ControllerRequestContextUtil.ANONYMOUS_CONTEXT;
import static org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextFor;
import static org.apache.kafka.controller.QuorumControllerIntegrationTestUtils.brokerFeatures;
import static org.apache.kafka.controller.QuorumControllerIntegrationTestUtils.brokerFeaturesPlusFeatureVersions;
import static org.apache.kafka.controller.QuorumControllerIntegrationTestUtils.pause;
import static org.apache.kafka.controller.QuorumControllerIntegrationTestUtils.registerBrokersAndUnfence;
import static org.apache.kafka.controller.QuorumControllerIntegrationTestUtils.sendBrokerHeartbeatToUnfenceBrokers;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class QuorumControllerTest {
    private static final Logger log = LoggerFactory.getLogger(QuorumControllerTest.class);

    static final BootstrapMetadata SIMPLE_BOOTSTRAP = BootstrapMetadata.
            fromVersion(MetadataVersion.IBP_3_7_IV0, "test-provided bootstrap");

    /**
     * Test setting some configuration values and reading them back.
     */
    @Test
    public void testConfigurationOperations() throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                build()
        ) {
            controlEnv.activeController().registerBroker(ANONYMOUS_CONTEXT,
                new BrokerRegistrationRequestData().
                setFeatures(brokerFeaturesPlusFeatureVersions(MetadataVersion.IBP_3_0_IV1, MetadataVersion.latestTesting(),
                    Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME, EligibleLeaderReplicasVersion.ELRV_1.featureLevel()))).
                setBrokerId(0).
                setLogDirs(Collections.singletonList(Uuid.fromString("iiaQjkRPQcuMULNII0MUeA"))).
                setClusterId(logEnv.clusterId())).get();
            testConfigurationOperations(controlEnv.activeController());

            testToImages(logEnv.allRecords());
        }
    }

    private void testConfigurationOperations(QuorumController controller) throws Throwable {
        assertEquals(Collections.singletonMap(BROKER0, ApiError.NONE),
            controller.incrementalAlterConfigs(ANONYMOUS_CONTEXT, Collections.singletonMap(
                BROKER0, Collections.singletonMap("baz", entry(SET, "123"))), true).get());
        assertEquals(Collections.singletonMap(BROKER0,
            new ResultOrError<>(Collections.emptyMap())),
            controller.describeConfigs(ANONYMOUS_CONTEXT, Collections.singletonMap(
                BROKER0, Collections.emptyList())).get());
        assertEquals(Collections.singletonMap(BROKER0, ApiError.NONE),
            controller.incrementalAlterConfigs(ANONYMOUS_CONTEXT, Collections.singletonMap(
                BROKER0, Collections.singletonMap("baz", entry(SET, "123"))), false).get());
        assertEquals(Collections.singletonMap(BROKER0, new ResultOrError<>(Collections.
                singletonMap("baz", "123"))),
            controller.describeConfigs(ANONYMOUS_CONTEXT, Collections.singletonMap(
                BROKER0, Collections.emptyList())).get());
    }

    /**
     * Test that an incrementalAlterConfigs operation doesn't complete until the records
     * can be written to the metadata log.
     */
    @Test
    public void testDelayedConfigurationOperations() throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                build()
        ) {
            controlEnv.activeController().registerBroker(ANONYMOUS_CONTEXT,
                new BrokerRegistrationRequestData().
                    setFeatures(brokerFeaturesPlusFeatureVersions(MetadataVersion.IBP_3_0_IV1, MetadataVersion.latestTesting(),
                        Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME, EligibleLeaderReplicasVersion.ELRV_1.featureLevel()))).
                    setBrokerId(0).
                    setLogDirs(Collections.singletonList(Uuid.fromString("sTbzRAMnTpahIyIPNjiLhw"))).
                    setClusterId(logEnv.clusterId())).get();
            testDelayedConfigurationOperations(logEnv, controlEnv.activeController());

            testToImages(logEnv.allRecords());
        }
    }

    private void testDelayedConfigurationOperations(
        LocalLogManagerTestEnv logEnv,
        QuorumController controller
    ) throws Throwable {
        logEnv.logManagers().forEach(m -> m.setMaxReadOffset(1L));
        CompletableFuture<Map<ConfigResource, ApiError>> future1 =
            controller.incrementalAlterConfigs(ANONYMOUS_CONTEXT, Collections.singletonMap(
                BROKER0, Collections.singletonMap("baz", entry(SET, "123"))), false);
        assertFalse(future1.isDone());
        assertEquals(Collections.singletonMap(BROKER0,
            new ResultOrError<>(Collections.emptyMap())),
            controller.describeConfigs(ANONYMOUS_CONTEXT, Collections.singletonMap(
                BROKER0, Collections.emptyList())).get());
        logEnv.logManagers().forEach(m -> m.setMaxReadOffset(6L));
        assertEquals(Collections.singletonMap(BROKER0, ApiError.NONE), future1.get());
    }

    @Test
    public void testFenceMultipleBrokers() throws Throwable {
        List<Integer> allBrokers = Arrays.asList(1, 2, 3, 4, 5);
        List<Integer> brokersToKeepUnfenced = Arrays.asList(1);
        List<Integer> brokersToFence = Arrays.asList(2, 3, 4, 5);
        short replicationFactor = (short) allBrokers.size();
        short numberOfPartitions = (short) allBrokers.size();
        long sessionTimeoutMillis = 1000;

        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setSessionTimeoutMillis(OptionalLong.of(sessionTimeoutMillis)).
                setBootstrapMetadata(SIMPLE_BOOTSTRAP).
                build()
        ) {
            ListenerCollection listeners = new ListenerCollection();
            listeners.add(new Listener().setName("PLAINTEXT").setHost("localhost").setPort(9092));
            QuorumController active = controlEnv.activeController();
            Map<Integer, Long> brokerEpochs = new HashMap<>();

            for (Integer brokerId : allBrokers) {
                CompletableFuture<BrokerRegistrationReply> reply = active.registerBroker(
                    ANONYMOUS_CONTEXT,
                    new BrokerRegistrationRequestData().
                        setBrokerId(brokerId).
                        setClusterId(active.clusterId()).
                        setFeatures(brokerFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.latestTesting())).
                        setIncarnationId(Uuid.randomUuid()).
                        setListeners(listeners));
                brokerEpochs.put(brokerId, reply.get().epoch());
            }

            // Brokers are only registered and should still be fenced
            allBrokers.forEach(brokerId ->
                assertFalse(active.clusterControl().isUnfenced(brokerId), "Broker " + brokerId + " should have been fenced")
            );

            // Unfence all brokers and create a topic foo
            sendBrokerHeartbeatToUnfenceBrokers(active, allBrokers, brokerEpochs);
            CreateTopicsRequestData createTopicsRequestData = new CreateTopicsRequestData().setTopics(
                new CreatableTopicCollection(Collections.singleton(
                    new CreatableTopic().setName("foo").setNumPartitions(numberOfPartitions).
                        setReplicationFactor(replicationFactor)).iterator()));
            CreateTopicsResponseData createTopicsResponseData = active.createTopics(
                ANONYMOUS_CONTEXT, createTopicsRequestData,
                Collections.singleton("foo")).get();
            assertEquals(Errors.NONE, Errors.forCode(createTopicsResponseData.topics().find("foo").errorCode()));
            Uuid topicIdFoo = createTopicsResponseData.topics().find("foo").topicId();

            // Fence some of the brokers
            TestUtils.waitForCondition(() -> {
                    sendBrokerHeartbeatToUnfenceBrokers(active, brokersToKeepUnfenced, brokerEpochs);
                    for (Integer brokerId : brokersToFence) {
                        if (active.clusterControl().isUnfenced(brokerId)) {
                            return false;
                        }
                    }
                    return true;
                }, sessionTimeoutMillis * 3,
                "Fencing of brokers did not process within expected time"
            );

            // Send another heartbeat to the brokers we want to keep alive
            sendBrokerHeartbeatToUnfenceBrokers(active, brokersToKeepUnfenced, brokerEpochs);

            // At this point only the brokers we want fenced should be fenced.
            brokersToKeepUnfenced.forEach(brokerId ->
                assertTrue(active.clusterControl().isUnfenced(brokerId), "Broker " + brokerId + " should have been unfenced")
            );
            brokersToFence.forEach(brokerId ->
                assertFalse(active.clusterControl().isUnfenced(brokerId), "Broker " + brokerId + " should have been fenced")
            );

            // Verify the isr and leaders for the topic partition
            int[] expectedIsr = {1};
            int[] isrFoo = active.replicationControl().getPartition(topicIdFoo, 0).isr;

            assertArrayEquals(isrFoo, expectedIsr, "The ISR for topic foo was " + Arrays.toString(isrFoo) +
                    ". It is expected to be " + Arrays.toString(expectedIsr));

            int fooLeader = active.replicationControl().getPartition(topicIdFoo, 0).leader;
            assertEquals(expectedIsr[0], fooLeader);

            // Check that there are imbalaned partitions
            assertTrue(active.replicationControl().arePartitionLeadersImbalanced());

            testToImages(logEnv.allRecords());
        }
    }

    @Test
    public void testUncleanShutdownBrokerElrEnabled() throws Throwable {
        List<Integer> allBrokers = Arrays.asList(1, 2, 3);
        short replicationFactor = (short) allBrokers.size();
        long sessionTimeoutMillis = 500;

        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setSessionTimeoutMillis(OptionalLong.of(sessionTimeoutMillis)).
                setBootstrapMetadata(BootstrapMetadata.fromVersion(MetadataVersion.IBP_4_0_IV1, "test-provided bootstrap ELR enabled")).
                build()
        ) {
            ListenerCollection listeners = new ListenerCollection();
            listeners.add(new Listener().setName("PLAINTEXT").setHost("localhost").setPort(9092));
            QuorumController active = controlEnv.activeController();
            Map<Integer, Long> brokerEpochs = new HashMap<>();
            BrokerRegistrationRequestData.FeatureCollection features =
                brokerFeaturesPlusFeatureVersions(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_4_0_IV1,
                    Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME, EligibleLeaderReplicasVersion.ELRV_1.featureLevel()));
            for (Integer brokerId : allBrokers) {
                CompletableFuture<BrokerRegistrationReply> reply = active.registerBroker(
                    anonymousContextFor(ApiKeys.BROKER_REGISTRATION),
                    new BrokerRegistrationRequestData().
                        setBrokerId(brokerId).
                        setClusterId(active.clusterId()).
                        setFeatures(features).
                        setIncarnationId(Uuid.randomUuid()).
                        setLogDirs(Collections.singletonList(Uuid.randomUuid())).
                        setListeners(listeners));
                brokerEpochs.put(brokerId, reply.get().epoch());
            }

            // Brokers are only registered and should still be fenced
            allBrokers.forEach(brokerId ->
                assertFalse(active.clusterControl().isUnfenced(brokerId), "Broker " + brokerId + " should have been fenced")
            );

            // Unfence all brokers and create a topic foo
            sendBrokerHeartbeatToUnfenceBrokers(active, allBrokers, brokerEpochs);
            CreateTopicsRequestData createTopicsRequestData = new CreateTopicsRequestData().setTopics(
                new CreatableTopicCollection(Collections.singleton(
                    new CreatableTopic().setName("foo").setNumPartitions(1).
                        setReplicationFactor(replicationFactor)).iterator()));
            CreateTopicsResponseData createTopicsResponseData = active.createTopics(
                ANONYMOUS_CONTEXT, createTopicsRequestData,
                Collections.singleton("foo")).get();
            assertEquals(Errors.NONE, Errors.forCode(createTopicsResponseData.topics().find("foo").errorCode()));
            Uuid topicIdFoo = createTopicsResponseData.topics().find("foo").topicId();
            ConfigRecord configRecord = new ConfigRecord()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName("foo")
                .setName(org.apache.kafka.common.config.TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)
                .setValue("2");
            RecordTestUtils.replayAll(active.configurationControl(), singletonList(new ApiMessageAndVersion(configRecord, (short) 0)));

            // Fence all the brokers
            TestUtils.waitForCondition(() -> {
                    for (Integer brokerId : allBrokers) {
                        if (active.clusterControl().isUnfenced(brokerId)) {
                            return false;
                        }
                    }
                    return true;
                }, sessionTimeoutMillis * 30,
                "Fencing of brokers did not process within expected time"
            );

            // Verify the isr and elr for the topic partition
            PartitionRegistration partition = active.replicationControl().getPartition(topicIdFoo, 0);
            assertEquals(1, partition.lastKnownElr.length, partition.toString());
            int[] lastKnownElr = partition.lastKnownElr;
            assertEquals(0, partition.isr.length, partition.toString());
            assertEquals(NO_LEADER, partition.leader, partition.toString());

            // The ELR set is not determined.
            assertEquals(2, partition.elr.length, partition.toString());
            int brokerToUncleanShutdown, brokerToBeTheLeader;

            // lastKnownElr stores the last known leader.
            if (lastKnownElr[0] == partition.elr[0]) {
                brokerToUncleanShutdown = partition.elr[0];
                brokerToBeTheLeader = partition.elr[1];
            } else {
                brokerToUncleanShutdown = partition.elr[1];
                brokerToBeTheLeader = partition.elr[0];
            }

            // Unclean shutdown should remove the ELR members.
            active.registerBroker(
                anonymousContextFor(ApiKeys.BROKER_REGISTRATION),
                new BrokerRegistrationRequestData().
                    setBrokerId(brokerToUncleanShutdown).
                    setClusterId(active.clusterId()).
                    setFeatures(features).
                    setIncarnationId(Uuid.randomUuid()).
                    setLogDirs(Collections.singletonList(Uuid.randomUuid())).
                    setListeners(listeners)).get();
            partition = active.replicationControl().getPartition(topicIdFoo, 0);
            assertArrayEquals(new int[]{brokerToBeTheLeader}, partition.elr, partition.toString());

            // Unclean shutdown should not remove the last known ELR members.
            active.registerBroker(
                anonymousContextFor(ApiKeys.BROKER_REGISTRATION),
                new BrokerRegistrationRequestData().
                    setBrokerId(lastKnownElr[0]).
                    setClusterId(active.clusterId()).
                    setFeatures(features).
                    setIncarnationId(Uuid.randomUuid()).
                    setLogDirs(Collections.singletonList(Uuid.randomUuid())).
                    setListeners(listeners)).get();
            partition = active.replicationControl().getPartition(topicIdFoo, 0);
            assertArrayEquals(lastKnownElr, partition.lastKnownElr, partition.toString());

            // Unfence the last one in the ELR, it should be elected.
            sendBrokerHeartbeatToUnfenceBrokers(active, singletonList(brokerToBeTheLeader), brokerEpochs);
            TestUtils.waitForCondition(() -> active.clusterControl().isUnfenced(brokerToBeTheLeader), sessionTimeoutMillis * 3,
                "Broker should be unfenced."
            );

            partition = active.replicationControl().getPartition(topicIdFoo, 0);
            assertArrayEquals(new int[]{brokerToBeTheLeader}, partition.isr, partition.toString());
            assertEquals(0, partition.elr.length, partition.toString());
            assertEquals(0, partition.lastKnownElr.length, partition.toString());
            assertEquals(brokerToBeTheLeader, partition.leader, partition.toString());
        }
    }

    @Test
    public void testUncleanShutdownElrDisabled() throws Exception {
        List<Integer> allBrokers = Arrays.asList(1, 2, 3);
        short replicationFactor = (short) allBrokers.size();
        long sessionTimeoutMillis = 500;

        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv)
                .setControllerBuilderInitializer(controllerBuilder ->
                    controllerBuilder.setFenceStaleBrokerIntervalNs(TimeUnit.SECONDS.toNanos(15)))
                .setSessionTimeoutMillis(OptionalLong.of(sessionTimeoutMillis))
                .setBootstrapMetadata(BootstrapMetadata.fromVersion(MetadataVersion.IBP_4_0_IV0, "test-provided bootstrap ELR not supported"))
                .build()
        ) {
            ListenerCollection listeners = new ListenerCollection();
            listeners.add(new Listener().setName("PLAINTEXT").setHost("localhost").setPort(9092));
            QuorumController active = controlEnv.activeController();
            Map<Integer, Long> brokerEpochs = new HashMap<>();
            BrokerRegistrationRequestData.FeatureCollection features =
                brokerFeaturesPlusFeatureVersions(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_4_0_IV0,
                    Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME, EligibleLeaderReplicasVersion.ELRV_0.featureLevel()));
            for (Integer brokerId : allBrokers) {
                CompletableFuture<BrokerRegistrationReply> reply = active.registerBroker(
                    anonymousContextFor(ApiKeys.BROKER_REGISTRATION),
                    new BrokerRegistrationRequestData().
                        setBrokerId(brokerId).
                        setClusterId(active.clusterId()).
                        setFeatures(features).
                        setIncarnationId(Uuid.randomUuid()).
                        setLogDirs(Collections.singletonList(Uuid.randomUuid())).
                        setListeners(listeners));
                brokerEpochs.put(brokerId, reply.get().epoch());
            }

            // Brokers are only registered and should still be fenced
            allBrokers.forEach(brokerId ->
                assertFalse(active.clusterControl().isUnfenced(brokerId), "Broker " + brokerId + " should have been fenced")
            );

            // Unfence all brokers and create a topic foo
            sendBrokerHeartbeatToUnfenceBrokers(active, allBrokers, brokerEpochs);
            CreateTopicsRequestData createTopicsRequestData = new CreateTopicsRequestData().setTopics(
                new CreatableTopicCollection(Collections.singleton(
                    new CreatableTopic().setName("foo").setNumPartitions(1).
                        setReplicationFactor(replicationFactor)).iterator()));
            CreateTopicsResponseData createTopicsResponseData = active.createTopics(
                ANONYMOUS_CONTEXT, createTopicsRequestData,
                Collections.singleton("foo")).get();
            assertEquals(Errors.NONE, Errors.forCode(createTopicsResponseData.topics().find("foo").errorCode()));
            Uuid topicIdFoo = createTopicsResponseData.topics().find("foo").topicId();

            // wait for brokers to become inactive
            active.time().sleep(sessionTimeoutMillis);

            // unclean shutdown for each replica
            for (int i = 0; i < (int) replicationFactor; i++) {
                // Verify that ELR is disabled
                PartitionRegistration partition = active.replicationControl().getPartition(topicIdFoo, 0);
                assertEquals(0, partition.elr.length, partition.toString());
                assertEquals(0, partition.lastKnownElr.length, partition.toString());

                boolean lastStandingIsr = i == (replicationFactor - 1);
                int prevLeader = partition.leader;
                int prevLeaderEpoch = partition.leaderEpoch;
                // Unclean shutdown should remove the broker from the ISR and reassign leadership
                active.registerBroker(
                    anonymousContextFor(ApiKeys.BROKER_REGISTRATION),
                    new BrokerRegistrationRequestData().
                        setBrokerId(prevLeader).
                        setClusterId(active.clusterId()).
                        setFeatures(features).
                        setIncarnationId(Uuid.randomUuid()).
                        setLogDirs(Collections.singletonList(Uuid.randomUuid())).
                        setListeners(listeners)).get();
                partition = active.replicationControl().getPartition(topicIdFoo, 0);
                // leader should always change, leader epoch should always be incremented
                int currentLeader = partition.leader;
                int currentLeaderEpoch = partition.leaderEpoch;
                assertNotEquals(currentLeader, prevLeader);
                assertNotEquals(currentLeaderEpoch, prevLeaderEpoch);
                // if the broker is not the last standing ISR, it should be removed from the ISR
                if (lastStandingIsr) {
                    assertArrayEquals(new int[]{prevLeader}, partition.isr);
                    assertEquals(NO_LEADER, currentLeader);
                } else {
                    List<Integer> isr = Arrays.stream(partition.isr).boxed().toList();
                    assertFalse(isr.contains(prevLeader));
                }
            }
        }
    }

    @Test
    public void testBalancePartitionLeaders() throws Throwable {
        List<Integer> allBrokers = Arrays.asList(1, 2, 3);
        List<Integer> brokersToKeepUnfenced = Arrays.asList(1, 2);
        List<Integer> brokersToFence = Collections.singletonList(3);
        short replicationFactor = (short) allBrokers.size();
        short numberOfPartitions = (short) allBrokers.size();
        long sessionTimeoutMillis = 2000;
        long leaderImbalanceCheckIntervalNs = 1_000_000_000;

        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setSessionTimeoutMillis(OptionalLong.of(sessionTimeoutMillis)).
                setLeaderImbalanceCheckIntervalNs(OptionalLong.of(leaderImbalanceCheckIntervalNs)).
                setBootstrapMetadata(SIMPLE_BOOTSTRAP).
                build()
        ) {
            ListenerCollection listeners = new ListenerCollection();
            listeners.add(new Listener().setName("PLAINTEXT").setHost("localhost").setPort(9092));
            QuorumController active = controlEnv.activeController();
            Map<Integer, Long> brokerEpochs = new HashMap<>();

            for (Integer brokerId : allBrokers) {
                CompletableFuture<BrokerRegistrationReply> reply = active.registerBroker(
                    ANONYMOUS_CONTEXT,
                    new BrokerRegistrationRequestData().
                        setBrokerId(brokerId).
                        setClusterId(active.clusterId()).
                        setFeatures(brokerFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_7_IV0)).
                        setIncarnationId(Uuid.randomUuid()).
                        setListeners(listeners));
                brokerEpochs.put(brokerId, reply.get().epoch());
            }

            // Brokers are only registered and should still be fenced
            allBrokers.forEach(brokerId ->
                assertFalse(active.clusterControl().isUnfenced(brokerId), "Broker " + brokerId + " should have been fenced")
            );

            // Unfence all brokers and create a topic foo
            sendBrokerHeartbeatToUnfenceBrokers(active, allBrokers, brokerEpochs);
            CreateTopicsRequestData createTopicsRequestData = new CreateTopicsRequestData().setTopics(
                new CreatableTopicCollection(Collections.singleton(
                    new CreatableTopic().setName("foo").setNumPartitions(numberOfPartitions).
                        setReplicationFactor(replicationFactor)).iterator()));
            CreateTopicsResponseData createTopicsResponseData = active.createTopics(
                ANONYMOUS_CONTEXT, createTopicsRequestData, Collections.singleton("foo")).get();
            assertEquals(Errors.NONE, Errors.forCode(createTopicsResponseData.topics().find("foo").errorCode()));
            Uuid topicIdFoo = createTopicsResponseData.topics().find("foo").topicId();

            // Fence some of the brokers
            TestUtils.waitForCondition(
                () -> {
                    sendBrokerHeartbeatToUnfenceBrokers(active, brokersToKeepUnfenced, brokerEpochs);
                    for (Integer brokerId : brokersToFence) {
                        if (active.clusterControl().isUnfenced(brokerId)) {
                            return false;
                        }
                    }
                    return true;
                },
                sessionTimeoutMillis * 3,
                "Fencing of brokers did not process within expected time"
            );

            // Send another heartbeat to the brokers we want to keep alive
            sendBrokerHeartbeatToUnfenceBrokers(active, brokersToKeepUnfenced, brokerEpochs);

            // At this point only the brokers we want fenced should be fenced.
            brokersToKeepUnfenced.forEach(brokerId ->
                assertTrue(active.clusterControl().isUnfenced(brokerId), "Broker " + brokerId + " should have been unfenced")
            );
            brokersToFence.forEach(brokerId ->
                assertFalse(active.clusterControl().isUnfenced(brokerId), "Broker " + brokerId + " should have been fenced")
            );

            // Check that there are imbalanced partitions
            assertTrue(active.replicationControl().arePartitionLeadersImbalanced());

            // Re-register all fenced brokers
            for (Integer brokerId : brokersToFence) {
                CompletableFuture<BrokerRegistrationReply> reply = active.registerBroker(
                    ANONYMOUS_CONTEXT,
                    new BrokerRegistrationRequestData().
                        setBrokerId(brokerId).
                        setClusterId(active.clusterId()).
                        setFeatures(brokerFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_7_IV0)).
                        setIncarnationId(Uuid.randomUuid()).
                        setListeners(listeners));
                brokerEpochs.put(brokerId, reply.get().epoch());
            }

            // Unfence all brokers
            sendBrokerHeartbeatToUnfenceBrokers(active, allBrokers, brokerEpochs);

            // Let the unfenced broker, 3, join the ISR partition 2
            Set<TopicIdPartition> imbalancedPartitions = new HashSet<>(active.replicationControl().imbalancedPartitions());
            assertEquals(1, imbalancedPartitions.size());
            int imbalancedPartitionId = imbalancedPartitions.iterator().next().partitionId();
            PartitionRegistration partitionRegistration = active.replicationControl().getPartition(topicIdFoo, imbalancedPartitionId);
            AlterPartitionRequestData.PartitionData partitionData = new AlterPartitionRequestData.PartitionData()
                .setPartitionIndex(imbalancedPartitionId)
                .setLeaderEpoch(partitionRegistration.leaderEpoch)
                .setPartitionEpoch(partitionRegistration.partitionEpoch)
                .setNewIsrWithEpochs(AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(1, 2, 3)));

            AlterPartitionRequestData.TopicData topicData = new AlterPartitionRequestData.TopicData()
                .setTopicName("foo");
            topicData.partitions().add(partitionData);

            AlterPartitionRequestData alterPartitionRequest = new AlterPartitionRequestData()
                .setBrokerId(partitionRegistration.leader)
                .setBrokerEpoch(brokerEpochs.get(partitionRegistration.leader));
            alterPartitionRequest.topics().add(topicData);

            active.alterPartition(ANONYMOUS_CONTEXT, new AlterPartitionRequest
                .Builder(alterPartitionRequest, false).build((short) 0).data()).get();

            AtomicLong lastHeartbeatMs = new AtomicLong(getMonotonicMs(active.time()));
            sendBrokerHeartbeatToUnfenceBrokers(active, allBrokers, brokerEpochs);
            // Check that partitions are balanced
            TestUtils.waitForCondition(
                () -> {
                    long currentMonotonicMs = getMonotonicMs(active.time());
                    if (currentMonotonicMs > lastHeartbeatMs.get() + (sessionTimeoutMillis / 2)) {
                        lastHeartbeatMs.set(currentMonotonicMs);
                        sendBrokerHeartbeatToUnfenceBrokers(active, allBrokers, brokerEpochs);
                    }
                    return !active.replicationControl().arePartitionLeadersImbalanced();
                },
                TimeUnit.MILLISECONDS.convert(leaderImbalanceCheckIntervalNs * 10, TimeUnit.NANOSECONDS),
                "Leaders were not balanced after unfencing all of the brokers"
            );

            testToImages(logEnv.allRecords());
        }
    }

    private static long getMonotonicMs(Time time) {
        return TimeUnit.NANOSECONDS.toMillis(time.nanoseconds());
    }

    @Test
    public void testNoOpRecordWriteAfterTimeout() throws Throwable {
        long maxIdleIntervalNs = TimeUnit.MICROSECONDS.toNanos(100);
        long maxReplicationDelayMs = 1_000;
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setControllerBuilderInitializer(controllerBuilder ->
                    controllerBuilder.setMaxIdleIntervalNs(OptionalLong.of(maxIdleIntervalNs))
                ).
                build()
        ) {
            ListenerCollection listeners = new ListenerCollection();
            listeners.add(new Listener().setName("PLAINTEXT").setHost("localhost").setPort(9092));
            QuorumController active = controlEnv.activeController();

            LocalLogManager localLogManager = logEnv
                .logManagers()
                .stream()
                .filter(logManager -> logManager.nodeId().equals(OptionalInt.of(active.nodeId())))
                .findAny()
                .get();
            TestUtils.waitForCondition(
                () -> localLogManager.highWatermark().isPresent(),
                maxReplicationDelayMs,
                "High watermark was not established"
            );

            final long firstHighWatermark = localLogManager.highWatermark().getAsLong();
            TestUtils.waitForCondition(
                () -> localLogManager.highWatermark().getAsLong() > firstHighWatermark,
                maxReplicationDelayMs,
                "Active controller didn't write NoOpRecord the first time"
            );

            // Do it again to make sure that we are not counting the leader change record
            final long secondHighWatermark = localLogManager.highWatermark().getAsLong();
            TestUtils.waitForCondition(
                () -> localLogManager.highWatermark().getAsLong() > secondHighWatermark,
                maxReplicationDelayMs,
                "Active controller didn't write NoOpRecord the second time"
            );
        }
    }

    @ParameterizedTest
    @CsvSource(value = {"0, 0", "0, 1", "1, 0", "1, 1"})
    public void testRegisterBrokerKRaftVersions(short finalizedKraftVersion, short brokerMaxSupportedKraftVersion) throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                setLastKRaftVersion(KRaftVersion.fromFeatureLevel(finalizedKraftVersion)).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setBootstrapMetadata(SIMPLE_BOOTSTRAP).
                build()
        ) {
            ListenerCollection listeners = new ListenerCollection();
            listeners.add(new Listener().setName("PLAINTEXT").
                setHost("localhost").setPort(9092));
            QuorumController active = controlEnv.activeController();
            BrokerRegistrationRequestData.FeatureCollection brokerFeatures = new BrokerRegistrationRequestData.FeatureCollection();
            brokerFeatures.add(new BrokerRegistrationRequestData.Feature()
                .setName(MetadataVersion.FEATURE_NAME)
                .setMinSupportedVersion(MetadataVersion.IBP_3_0_IV1.featureLevel())
                .setMaxSupportedVersion(MetadataVersion.latestTesting().featureLevel()));
            // broker registration requests do not include initial versions of features
            if (brokerMaxSupportedKraftVersion != 0) {
                brokerFeatures.add(new BrokerRegistrationRequestData.Feature()
                    .setName(KRaftVersion.FEATURE_NAME)
                    .setMinSupportedVersion(Feature.KRAFT_VERSION.minimumProduction())
                    .setMaxSupportedVersion(brokerMaxSupportedKraftVersion));
            }
            BrokerRegistrationRequestData request = new BrokerRegistrationRequestData().
                setBrokerId(0).
                setClusterId(active.clusterId()).
                setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwBA")).
                setFeatures(brokerFeatures).
                setLogDirs(Collections.singletonList(Uuid.fromString("vBpaRsZVSaGsQT53wtYGtg"))).
                setListeners(listeners);

            if (brokerMaxSupportedKraftVersion < finalizedKraftVersion) {
                Throwable exception = assertThrows(ExecutionException.class, () -> active.registerBroker(
                    ANONYMOUS_CONTEXT,
                    request).get());
                assertEquals(UnsupportedVersionException.class, exception.getCause().getClass());
                assertEquals("Unable to register because the broker does not support finalized version " +
                        finalizedKraftVersion + " of kraft.version. The broker wants a version between 0 and " +
                        brokerMaxSupportedKraftVersion + ", inclusive.",
                    exception.getCause().getMessage());
            } else {
                BrokerRegistrationReply reply = active.registerBroker(
                    ANONYMOUS_CONTEXT,
                    request).get();
                assertTrue(reply.epoch() >= 4, "Unexpected broker epoch " + reply.epoch());
            }
        }
    }

    @Test
    public void testUnregisterBroker() throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                build()
        ) {
            ListenerCollection listeners = new ListenerCollection();
            listeners.add(new Listener().setName("PLAINTEXT").
                setHost("localhost").setPort(9092));
            QuorumController active = controlEnv.activeController();
            CompletableFuture<BrokerRegistrationReply> reply = active.registerBroker(
                ANONYMOUS_CONTEXT,
                new BrokerRegistrationRequestData().
                    setBrokerId(0).
                    setClusterId(active.clusterId()).
                    setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwBA")).
                    setFeatures(brokerFeaturesPlusFeatureVersions(MetadataVersion.IBP_3_0_IV1, MetadataVersion.latestTesting(),
                        Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME, EligibleLeaderReplicasVersion.ELRV_1.featureLevel()))).
                    setLogDirs(Collections.singletonList(Uuid.fromString("vBpaRsZVSaGsQT53wtYGtg"))).
                    setListeners(listeners));
            assertEquals(4L, reply.get().epoch());
            CreateTopicsRequestData createTopicsRequestData =
                new CreateTopicsRequestData().setTopics(
                    new CreatableTopicCollection(Collections.singleton(
                        new CreatableTopic().setName("foo").setNumPartitions(1).
                            setReplicationFactor((short) 1)).iterator()));
            assertEquals(Errors.INVALID_REPLICATION_FACTOR.code(), active.createTopics(
                ANONYMOUS_CONTEXT,
                createTopicsRequestData, Collections.singleton("foo")).get().
                    topics().find("foo").errorCode());
            assertEquals("Unable to replicate the partition 1 time(s): All brokers " +
                "are currently fenced.", active.createTopics(ANONYMOUS_CONTEXT,
                    createTopicsRequestData, Collections.singleton("foo")).
                        get().topics().find("foo").errorMessage());
            assertEquals(new BrokerHeartbeatReply(true, false, false, false),
                active.processBrokerHeartbeat(ANONYMOUS_CONTEXT, new BrokerHeartbeatRequestData().
                        setWantFence(false).setBrokerEpoch(4L).setBrokerId(0).
                        setCurrentMetadataOffset(100000L)).get());
            assertEquals(Errors.NONE.code(), active.createTopics(ANONYMOUS_CONTEXT,
                createTopicsRequestData, Collections.singleton("foo")).
                    get().topics().find("foo").errorCode());
            CompletableFuture<TopicIdPartition> topicPartitionFuture = active.appendReadEvent(
                "debugGetPartition", OptionalLong.empty(), () -> {
                    Iterator<TopicIdPartition> iterator = active.
                        replicationControl().brokersToIsrs().iterator(0, true);
                    assertTrue(iterator.hasNext());
                    return iterator.next();
                });
            assertEquals(0, topicPartitionFuture.get().partitionId());
            active.unregisterBroker(ANONYMOUS_CONTEXT, 0).get();
            topicPartitionFuture = active.appendReadEvent(
                "debugGetPartition", OptionalLong.empty(), () -> {
                    Iterator<TopicIdPartition> iterator = active.
                        replicationControl().brokersToIsrs().partitionsWithNoLeader();
                    assertTrue(iterator.hasNext());
                    return iterator.next();
                });
            assertEquals(0, topicPartitionFuture.get().partitionId());

            testToImages(logEnv.allRecords());
        }
    }

    private RegisterBrokerRecord.BrokerFeatureCollection registrationFeatures(
        MetadataVersion minVersion,
        MetadataVersion maxVersion
    ) {
        RegisterBrokerRecord.BrokerFeatureCollection features = new RegisterBrokerRecord.BrokerFeatureCollection();
        features.add(new RegisterBrokerRecord.BrokerFeature().
                setName(MetadataVersion.FEATURE_NAME).
                setMinSupportedVersion(minVersion.featureLevel()).
                setMaxSupportedVersion(maxVersion.featureLevel()));
        return features;
    }

    @Test
    public void testSnapshotSaveAndLoad() throws Throwable {
        final int numBrokers = 4;
        Map<Integer, Long> brokerEpochs = new HashMap<>();
        Uuid fooId;
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setBootstrapMetadata(SIMPLE_BOOTSTRAP).
                build()
        ) {
            QuorumController active = controlEnv.activeController();
            for (int i = 0; i < logEnv.logManagers().size(); i++) {
                active.registerController(ANONYMOUS_CONTEXT,
                    new ControllerRegistrationRequestData().
                        setControllerId(i).
                        setIncarnationId(new Uuid(3465346L, i)).
                        setZkMigrationReady(false).
                        setListeners(new ControllerRegistrationRequestData.ListenerCollection(
                            singletonList(
                                new ControllerRegistrationRequestData.Listener().
                                    setName("CONTROLLER").
                                    setHost("localhost").
                                    setPort(8000 + i).
                                    setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
                                ).iterator()
                        )).
                        setFeatures(new ControllerRegistrationRequestData.FeatureCollection(
                            singletonList(
                                new ControllerRegistrationRequestData.Feature().
                                    setName(MetadataVersion.FEATURE_NAME).
                                    setMinSupportedVersion(MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel()).
                                    setMaxSupportedVersion(MetadataVersion.IBP_3_7_IV0.featureLevel())
                            ).iterator()
                        ))).get();
            }
            for (int i = 0; i < numBrokers; i++) {
                BrokerRegistrationReply reply = active.registerBroker(ANONYMOUS_CONTEXT,
                    new BrokerRegistrationRequestData().
                        setBrokerId(i).
                        setRack(null).
                        setClusterId(active.clusterId()).
                        setFeatures(brokerFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_7_IV0)).
                        setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB" + i)).
                        setListeners(new ListenerCollection(singletonList(new Listener().
                            setName("PLAINTEXT").setHost("localhost").
                            setPort(9092 + i)).iterator()))).get();
                brokerEpochs.put(i, reply.epoch());
            }
            for (int i = 0; i < numBrokers - 1; i++) {
                assertEquals(new BrokerHeartbeatReply(true, false, false, false),
                    active.processBrokerHeartbeat(ANONYMOUS_CONTEXT, new BrokerHeartbeatRequestData().
                        setWantFence(false).setBrokerEpoch(brokerEpochs.get(i)).
                        setBrokerId(i).setCurrentMetadataOffset(100000L)).get());
            }
            CreateTopicsResponseData fooData = active.createTopics(ANONYMOUS_CONTEXT,
                new CreateTopicsRequestData().setTopics(
                    new CreatableTopicCollection(Collections.singleton(
                        new CreatableTopic().setName("foo").setNumPartitions(-1).
                            setReplicationFactor((short) -1).
                            setAssignments(new CreatableReplicaAssignmentCollection(
                                Arrays.asList(new CreatableReplicaAssignment().
                                    setPartitionIndex(0).
                                    setBrokerIds(Arrays.asList(0, 1, 2)),
                                    new CreatableReplicaAssignment().
                                        setPartitionIndex(1).
                                        setBrokerIds(Arrays.asList(1, 2, 0))).
                                            iterator()))).iterator())),
                Collections.singleton("foo")).get();
            fooId = fooData.topics().find("foo").topicId();
            active.allocateProducerIds(ANONYMOUS_CONTEXT,
                new AllocateProducerIdsRequestData().setBrokerId(0).setBrokerEpoch(brokerEpochs.get(0))).get();
            controlEnv.close();
            assertEquals(generateTestRecords(fooId, brokerEpochs), logEnv.allRecords());

            testToImages(logEnv.allRecords());
        }
    }

    private List<ApiMessageAndVersion> generateTestRecords(Uuid fooId, Map<Integer, Long> brokerEpochs) {
        return Arrays.asList(
            new ApiMessageAndVersion(new BeginTransactionRecord().
                setName("Bootstrap records"), (short) 0),
            new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(MetadataVersion.IBP_3_7_IV0.featureLevel()), (short) 0),
            new ApiMessageAndVersion(new EndTransactionRecord(), (short) 0),
            new ApiMessageAndVersion(new RegisterControllerRecord().
                setControllerId(0).
                setIncarnationId(Uuid.fromString("AAAAAAA04IIAAAAAAAAAAA")).
                setEndPoints(new RegisterControllerRecord.ControllerEndpointCollection(
                    singletonList(
                        new RegisterControllerRecord.ControllerEndpoint().
                            setName("CONTROLLER").
                            setHost("localhost").
                            setPort(8000).
                            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)).iterator())).
                setFeatures(new RegisterControllerRecord.ControllerFeatureCollection(
                    singletonList(
                        new RegisterControllerRecord.ControllerFeature().
                            setName(MetadataVersion.FEATURE_NAME).
                            setMinSupportedVersion(MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel()).
                            setMaxSupportedVersion(MetadataVersion.IBP_3_7_IV0.featureLevel())).iterator())),
                    (short) 0),
            new ApiMessageAndVersion(new RegisterControllerRecord().
                setControllerId(1).
                setIncarnationId(Uuid.fromString("AAAAAAA04IIAAAAAAAAAAQ")).
                setEndPoints(new RegisterControllerRecord.ControllerEndpointCollection(
                    singletonList(
                        new RegisterControllerRecord.ControllerEndpoint().
                            setName("CONTROLLER").
                            setHost("localhost").
                            setPort(8001).
                            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)).iterator())).
                setFeatures(new RegisterControllerRecord.ControllerFeatureCollection(
                    singletonList(
                        new RegisterControllerRecord.ControllerFeature().
                            setName(MetadataVersion.FEATURE_NAME).
                            setMinSupportedVersion(MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel()).
                            setMaxSupportedVersion(MetadataVersion.IBP_3_7_IV0.featureLevel())).iterator())),
                    (short) 0),
            new ApiMessageAndVersion(new RegisterControllerRecord().
                setControllerId(2).
                setIncarnationId(Uuid.fromString("AAAAAAA04IIAAAAAAAAAAg")).
                setEndPoints(new RegisterControllerRecord.ControllerEndpointCollection(
                    singletonList(
                        new RegisterControllerRecord.ControllerEndpoint().
                            setName("CONTROLLER").
                            setHost("localhost").
                            setPort(8002).
                            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)).iterator())).
                setFeatures(new RegisterControllerRecord.ControllerFeatureCollection(
                    singletonList(
                        new RegisterControllerRecord.ControllerFeature().
                            setName(MetadataVersion.FEATURE_NAME).
                            setMinSupportedVersion(MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel()).
                            setMaxSupportedVersion(MetadataVersion.IBP_3_7_IV0.featureLevel())).iterator())),
                (short) 0),
            new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerId(0).setBrokerEpoch(brokerEpochs.get(0)).
                setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB0")).
                setEndPoints(new BrokerEndpointCollection(
                    singletonList(new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").
                        setPort(9092).setSecurityProtocol((short) 0)).iterator())).
                setFeatures(registrationFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_7_IV0)).
                setRack(null).
                setFenced(true), (short) 2),
            new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerId(1).setBrokerEpoch(brokerEpochs.get(1)).
                setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB1")).
                setEndPoints(new BrokerEndpointCollection(singletonList(
                    new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").
                        setPort(9093).setSecurityProtocol((short) 0)).iterator())).
                setFeatures(registrationFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_7_IV0)).
                setRack(null).
                setFenced(true), (short) 2),
            new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerId(2).setBrokerEpoch(brokerEpochs.get(2)).
                setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB2")).
                setEndPoints(new BrokerEndpointCollection(
                    singletonList(new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").
                        setPort(9094).setSecurityProtocol((short) 0)).iterator())).
                setFeatures(registrationFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_7_IV0)).
                setRack(null).
                setFenced(true), (short) 2),
            new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerId(3).setBrokerEpoch(brokerEpochs.get(3)).
                setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB3")).
                setEndPoints(new BrokerEndpointCollection(singletonList(
                    new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").
                        setPort(9095).setSecurityProtocol((short) 0)).iterator())).
                setFeatures(registrationFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_7_IV0)).
                setRack(null).
                setFenced(true), (short) 2),
            new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
                setBrokerId(0).
                setBrokerEpoch(brokerEpochs.get(0)).
                setFenced(BrokerRegistrationFencingChange.UNFENCE.value()), (short) 0),
            new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
                setBrokerId(1).
                setBrokerEpoch(brokerEpochs.get(1)).
                setFenced(BrokerRegistrationFencingChange.UNFENCE.value()), (short) 0),
            new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
                setBrokerId(2).
                setBrokerEpoch(brokerEpochs.get(2)).
                setFenced(BrokerRegistrationFencingChange.UNFENCE.value()), (short) 0),
            new ApiMessageAndVersion(new TopicRecord().
                setName("foo").setTopicId(fooId), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord().setPartitionId(0).
                setTopicId(fooId).setReplicas(Arrays.asList(0, 1, 2)).
                setIsr(Arrays.asList(0, 1, 2)).setRemovingReplicas(Collections.emptyList()).
                setAddingReplicas(Collections.emptyList()).setLeader(0).setLeaderEpoch(0).
                setPartitionEpoch(0), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord().setPartitionId(1).
                setTopicId(fooId).setReplicas(Arrays.asList(1, 2, 0)).
                setIsr(Arrays.asList(1, 2, 0)).setRemovingReplicas(Collections.emptyList()).
                setAddingReplicas(Collections.emptyList()).setLeader(1).setLeaderEpoch(0).
                setPartitionEpoch(0), (short) 0),
            new ApiMessageAndVersion(new ProducerIdsRecord().
                setBrokerId(0).
                setBrokerEpoch(brokerEpochs.get(0)).
                setNextProducerId(1000), (short) 0));
    }

    /**
     * Test that certain controller operations time out if they stay on the controller
     * queue for too long.
     */
    @Test
    public void testTimeouts() throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                build()
        ) {
            QuorumController controller = controlEnv.activeController();
            CountDownLatch countDownLatch = pause(controller);
            long now = controller.time().nanoseconds();
            ControllerRequestContext context0 = new ControllerRequestContext(
                new RequestHeaderData(), KafkaPrincipal.ANONYMOUS, OptionalLong.of(now));
            CompletableFuture<CreateTopicsResponseData> createFuture =
                controller.createTopics(context0, new CreateTopicsRequestData().setTimeoutMs(0).
                    setTopics(new CreatableTopicCollection(Collections.singleton(
                        new CreatableTopic().setName("foo")).iterator())),
                    Collections.emptySet());
            CompletableFuture<Map<Uuid, ApiError>> deleteFuture =
                controller.deleteTopics(context0, Collections.singletonList(Uuid.ZERO_UUID));
            CompletableFuture<Map<String, ResultOrError<Uuid>>> findTopicIdsFuture =
                controller.findTopicIds(context0, Collections.singletonList("foo"));
            CompletableFuture<Map<Uuid, ResultOrError<String>>> findTopicNamesFuture =
                controller.findTopicNames(context0, Collections.singletonList(Uuid.ZERO_UUID));
            CompletableFuture<List<CreatePartitionsTopicResult>> createPartitionsFuture =
                controller.createPartitions(context0, Collections.singletonList(
                    new CreatePartitionsTopic()), false);
            CompletableFuture<ElectLeadersResponseData> electLeadersFuture =
                controller.electLeaders(context0, new ElectLeadersRequestData().setTimeoutMs(0).
                    setTopicPartitions(null));
            CompletableFuture<AlterPartitionReassignmentsResponseData> alterReassignmentsFuture =
                controller.alterPartitionReassignments(context0,
                    new AlterPartitionReassignmentsRequestData().setTimeoutMs(0).
                        setTopics(Collections.singletonList(new ReassignableTopic())));
            CompletableFuture<ListPartitionReassignmentsResponseData> listReassignmentsFuture =
                controller.listPartitionReassignments(context0,
                    new ListPartitionReassignmentsRequestData().setTopics(null).setTimeoutMs(0));
            while (controller.time().nanoseconds() == now) {
                Thread.sleep(0, 10);
            }
            countDownLatch.countDown();
            assertYieldsTimeout(createFuture);
            assertYieldsTimeout(deleteFuture);
            assertYieldsTimeout(findTopicIdsFuture);
            assertYieldsTimeout(findTopicNamesFuture);
            assertYieldsTimeout(createPartitionsFuture);
            assertYieldsTimeout(electLeadersFuture);
            assertYieldsTimeout(alterReassignmentsFuture);
            assertYieldsTimeout(listReassignmentsFuture);

            testToImages(logEnv.allRecords());
        }
    }

    private static void assertYieldsTimeout(Future<?> future) {
        assertEquals(TimeoutException.class, assertThrows(ExecutionException.class, future::get).getCause().getClass());
    }

    /**
     * Test that certain controller operations finish immediately without putting an event
     * on the controller queue, if there is nothing to do.
     */
    @Test
    public void testEarlyControllerResults() throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                build()
        ) {
            QuorumController controller = controlEnv.activeController();
            CountDownLatch countDownLatch = pause(controller);
            CompletableFuture<CreateTopicsResponseData> createFuture =
                controller.createTopics(ANONYMOUS_CONTEXT, new CreateTopicsRequestData().
                    setTimeoutMs(120000), Collections.emptySet());
            CompletableFuture<Map<Uuid, ApiError>> deleteFuture =
                controller.deleteTopics(ANONYMOUS_CONTEXT, Collections.emptyList());
            CompletableFuture<Map<String, ResultOrError<Uuid>>> findTopicIdsFuture =
                controller.findTopicIds(ANONYMOUS_CONTEXT, Collections.emptyList());
            CompletableFuture<Map<Uuid, ResultOrError<String>>> findTopicNamesFuture =
                controller.findTopicNames(ANONYMOUS_CONTEXT, Collections.emptyList());
            CompletableFuture<List<CreatePartitionsTopicResult>> createPartitionsFuture =
                controller.createPartitions(ANONYMOUS_CONTEXT, Collections.emptyList(), false);
            CompletableFuture<ElectLeadersResponseData> electLeadersFuture =
                controller.electLeaders(ANONYMOUS_CONTEXT, new ElectLeadersRequestData());
            CompletableFuture<AlterPartitionReassignmentsResponseData> alterReassignmentsFuture =
                controller.alterPartitionReassignments(ANONYMOUS_CONTEXT,
                    new AlterPartitionReassignmentsRequestData());
            createFuture.get();
            deleteFuture.get();
            findTopicIdsFuture.get();
            findTopicNamesFuture.get();
            createPartitionsFuture.get();
            electLeadersFuture.get();
            alterReassignmentsFuture.get();
            countDownLatch.countDown();

            testToImages(logEnv.allRecords());
        }
    }

    @Test
    public void testConfigResourceExistenceChecker() throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                build()
        ) {
            QuorumController active = controlEnv.activeController();
            registerBrokersAndUnfence(active, 5);
            active.createTopics(ANONYMOUS_CONTEXT, new CreateTopicsRequestData().
                setTopics(new CreatableTopicCollection(Collections.singleton(
                    new CreatableTopic().setName("foo").
                        setReplicationFactor((short) 3).
                        setNumPartitions(1)).iterator())),
                Collections.singleton("foo")).get();
            ConfigResourceExistenceChecker checker =
                active.new ConfigResourceExistenceChecker();
            // A ConfigResource with type=BROKER and name=(empty string) represents
            // the default broker resource. It is used to set cluster configs.
            checker.accept(new ConfigResource(BROKER, ""));

            // Broker 3 exists, so we can set a configuration for it.
            checker.accept(new ConfigResource(BROKER, "3"));

            // Broker 10 does not exist, so this should throw an exception.
            assertThrows(BrokerIdNotRegisteredException.class,
                () -> checker.accept(new ConfigResource(BROKER, "10")));

            // Topic foo exists, so we can set a configuration for it.
            checker.accept(new ConfigResource(TOPIC, "foo"));

            // Topic bar does not exist, so this should throw an exception.
            assertThrows(UnknownTopicOrPartitionException.class,
                () -> checker.accept(new ConfigResource(TOPIC, "bar")));

            testToImages(logEnv.allRecords());
        }
    }

    private static final Uuid FOO_ID = Uuid.fromString("igRktLOnR8ektWHr79F8mw");

    private static final Map<Integer, Long> ALL_ZERO_BROKER_EPOCHS =
        IntStream.of(0, 1, 2, 3).boxed().collect(Collectors.toMap(identity(), __ -> 0L));

    @Test
    public void testFatalMetadataReplayErrorOnActive() throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                build()
        ) {
            QuorumController active = controlEnv.activeController();
            CompletableFuture<Void> future = active.appendWriteEvent("errorEvent",
                    OptionalLong.empty(), () -> ControllerResult.of(Collections.singletonList(new ApiMessageAndVersion(
                            new ConfigRecord().
                                    setName(null).
                                    setResourceName(null).
                                    setResourceType((byte) 255).
                                    setValue(null), (short) 0)), null));
            assertThrows(ExecutionException.class, future::get);
            assertEquals(NullPointerException.class, controlEnv.fatalFaultHandler(active.nodeId())
                .firstException().getCause().getClass());
            controlEnv.ignoreFatalFaults();
        }
    }

    @Test
    public void testFatalMetadataErrorDuringSnapshotLoading() throws Exception {
        InitialSnapshot invalidSnapshot = new InitialSnapshot(singletonList(
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0))
        );

        LocalLogManagerTestEnv.Builder logEnvBuilder = new LocalLogManagerTestEnv.Builder(3)
            .setSnapshotReader(FileRawSnapshotReader.open(
                invalidSnapshot.tempDir.toPath(),
                new OffsetAndEpoch(0, 0)
            ));

        try (LocalLogManagerTestEnv logEnv = logEnvBuilder.build()) {
            try (QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).build()) {
                TestUtils.waitForCondition(() -> controlEnv.controllers().stream().allMatch(
                        controller -> controlEnv.fatalFaultHandler(controller.nodeId()).firstException() != null),
                        "At least one controller failed to detect the fatal fault"
                );
                controlEnv.ignoreFatalFaults();
            }
        }
    }

    @Test
    public void testFatalMetadataErrorDuringLogLoading() throws Exception {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).build()) {
            logEnv.appendInitialRecords(singletonList(
                    new ApiMessageAndVersion(new PartitionRecord(), (short) 0)));

            try (QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).build()) {
                TestUtils.waitForCondition(() -> controlEnv.controllers().stream().allMatch(
                        controller -> controlEnv.fatalFaultHandler(controller.nodeId()).firstException() != null),
                        "At least one controller failed to detect the fatal fault"
                );
                controlEnv.ignoreFatalFaults();
            }
        }
    }

    private static void assertInitialLoadFuturesNotComplete(List<StandardAuthorizer> authorizers) {
        for (int i = 0; i < authorizers.size(); i++) {
            assertFalse(authorizers.get(i).initialLoadFuture().isDone(),
                "authorizer " + i + " should not have completed loading.");
        }
    }

    static class InitialSnapshot implements AutoCloseable {
        File tempDir;
        BatchFileWriter writer;

        public InitialSnapshot(List<ApiMessageAndVersion> records) throws Exception {
            tempDir = TestUtils.tempDirectory();
            Path path = Snapshots.snapshotPath(tempDir.toPath(), new OffsetAndEpoch(0, 0));
            writer = BatchFileWriter.open(path);
            writer.append(records);
            writer.close();
            writer = null;
        }

        @Override
        public void close() throws Exception {
            Utils.closeQuietly(writer, "BatchFileWriter");
            Utils.delete(tempDir);
        }
    }

    private static final List<ApiMessageAndVersion> PRE_PRODUCTION_RECORDS =
            Collections.unmodifiableList(Arrays.asList(
                new ApiMessageAndVersion(new RegisterBrokerRecord().
                        setBrokerEpoch(42).
                        setBrokerId(123).
                        setIncarnationId(Uuid.fromString("v78Gbc6sQXK0y5qqRxiryw")).
                        setRack(null),
                        (short) 0),
                new ApiMessageAndVersion(new UnfenceBrokerRecord().
                        setEpoch(42).
                        setId(123),
                        (short) 0),
                new ApiMessageAndVersion(new TopicRecord().
                        setName("bar").
                        setTopicId(Uuid.fromString("cxBT72dK4si8Ied1iP4wBA")),
                        (short) 0)));

    private static final BootstrapMetadata COMPLEX_BOOTSTRAP = BootstrapMetadata.fromRecords(
            Arrays.asList(
                new ApiMessageAndVersion(new FeatureLevelRecord().
                        setName(MetadataVersion.FEATURE_NAME).
                        setFeatureLevel(MetadataVersion.IBP_3_3_IV1.featureLevel()),
                        (short) 0),
                new ApiMessageAndVersion(new ConfigRecord().
                        setResourceType(BROKER.id()).
                        setResourceName("").
                        setName("foo").
                        setValue("bar"),
                        (short) 0)),
            "test bootstrap");

    @Test
    public void testUpgradeFromPreProductionVersion() throws Exception {
        try (
            InitialSnapshot initialSnapshot = new InitialSnapshot(PRE_PRODUCTION_RECORDS);
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                setSnapshotReader(FileRawSnapshotReader.open(
                    initialSnapshot.tempDir.toPath(), new OffsetAndEpoch(0, 0))).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setBootstrapMetadata(COMPLEX_BOOTSTRAP).
                build()
        ) {
            QuorumController active = controlEnv.activeController();
            TestUtils.waitForCondition(() ->
                active.featureControl().metadataVersion().equals(MetadataVersion.IBP_3_0_IV1),
                "Failed to get a metadata.version of " + MetadataVersion.IBP_3_0_IV1);
            // The ConfigRecord in our bootstrap should not have been applied, since there
            // were already records present.
            assertEquals(Collections.emptyMap(), active.configurationControl().
                    getConfigs(new ConfigResource(BROKER, "")));

            testToImages(logEnv.allRecords());
        }
    }

    @Test
    public void testInsertBootstrapRecordsToEmptyLog() throws Exception {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setBootstrapMetadata(COMPLEX_BOOTSTRAP).
                build()
        ) {
            QuorumController active = controlEnv.activeController();

            ControllerRequestContext ctx = new ControllerRequestContext(
                new RequestHeaderData(), KafkaPrincipal.ANONYMOUS, OptionalLong.of(Long.MAX_VALUE));

            TestUtils.waitForCondition(() -> {
                FinalizedControllerFeatures features = active.finalizedFeatures(ctx).get();
                Optional<Short> metadataVersionOpt = features.get(MetadataVersion.FEATURE_NAME);
                return Optional.of(MetadataVersion.IBP_3_3_IV1.featureLevel()).equals(metadataVersionOpt);
            }, "Failed to see expected metadata.version from bootstrap metadata");

            TestUtils.waitForCondition(() -> {
                ConfigResource defaultBrokerResource = new ConfigResource(BROKER, "");

                Map<ConfigResource, Collection<String>> configs = Collections.singletonMap(
                    defaultBrokerResource,
                    Collections.emptyList()
                );

                Map<ConfigResource, ResultOrError<Map<String, String>>> results =
                    active.describeConfigs(ctx, configs).get();

                ResultOrError<Map<String, String>> resultOrError = results.get(defaultBrokerResource);
                return resultOrError.isResult() &&
                    Collections.singletonMap("foo", "bar").equals(resultOrError.result());
            }, "Failed to see expected config change from bootstrap metadata");

            testToImages(logEnv.allRecords());
        }
    }

    static class TestAppender implements Function<List<ApiMessageAndVersion>, Long>  {
        private long offset = 0;

        @Override
        public Long apply(List<ApiMessageAndVersion> apiMessageAndVersions) {
            for (ApiMessageAndVersion apiMessageAndVersion : apiMessageAndVersions) {
                BrokerRegistrationChangeRecord record =
                        (BrokerRegistrationChangeRecord) apiMessageAndVersion.message();
                assertEquals((int) offset, record.brokerId());
                offset++;
            }
            return offset;
        }
    }

    private static ApiMessageAndVersion rec(int i) {
        return new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().setBrokerId(i),
                (short) 0);
    }

    @Test
    public void testAppendRecords() {
        TestAppender appender = new TestAppender();
        assertEquals(5, QuorumController.appendRecords(log,
            ControllerResult.of(Arrays.asList(rec(0), rec(1), rec(2), rec(3), rec(4)), null),
            2,
            appender));
    }

    @Test
    public void testAppendRecordsAtomically() {
        TestAppender appender = new TestAppender();
        assertEquals("Attempted to atomically commit 5 records, but maxRecordsPerBatch is 2",
            assertThrows(IllegalStateException.class, () ->
                QuorumController.appendRecords(log,
                        ControllerResult.atomicOf(Arrays.asList(rec(0), rec(1), rec(2), rec(3), rec(4)), null),
                        2,
                        appender)).getMessage());
    }

    FeatureControlManager getActivationRecords(MetadataVersion metadataVersion) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControlManager = new FeatureControlManager.Builder()
                .setSnapshotRegistry(snapshotRegistry)
                .setMetadataVersion(metadataVersion)
                .build();

        ControllerResult<Void> result = ActivationRecordsGenerator.generate(
            msg -> { },
            true,
            -1L,
            BootstrapMetadata.fromVersion(metadataVersion, "test"),
            metadataVersion);
        RecordTestUtils.replayAll(featureControlManager, result.records());
        return featureControlManager;
    }

    @Test
    public void testActivationRecords33() {
        FeatureControlManager featureControl;

        featureControl = getActivationRecords(MetadataVersion.IBP_3_3_IV0);
        assertEquals(MetadataVersion.IBP_3_3_IV0, featureControl.metadataVersion());
    }

    @Test
    public void testActivationRecords34() {
        FeatureControlManager featureControl;

        featureControl = getActivationRecords(MetadataVersion.IBP_3_4_IV0);
        assertEquals(MetadataVersion.IBP_3_4_IV0, featureControl.metadataVersion());
    }

    @Test
    public void testActivationRecordsNonEmptyLog() {
        FeatureControlManager featureControl = getActivationRecords(
            MetadataVersion.IBP_3_9_IV0);
        assertEquals(MetadataVersion.IBP_3_9_IV0, featureControl.metadataVersion());
    }

    @Test
    public void testActivationRecordsPartialBootstrap() {
        ControllerResult<Void> result = ActivationRecordsGenerator.generate(
            logMsg -> { },
            true,
            0L,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_6_IV1, "test"),
            MetadataVersion.IBP_3_6_IV1);
        assertFalse(result.isAtomic());
        assertTrue(RecordTestUtils.recordAtIndexAs(
            AbortTransactionRecord.class, result.records(), 0).isPresent());
        assertTrue(RecordTestUtils.recordAtIndexAs(
            BeginTransactionRecord.class, result.records(), 1).isPresent());
        assertTrue(RecordTestUtils.recordAtIndexAs(
            EndTransactionRecord.class, result.records(), result.records().size() - 1).isPresent());
    }

    /**
     * Tests all intermediate images lead to the same final image for each image & delta type.
     * @param fromRecords
     */
    @SuppressWarnings("unchecked")
    private static void testToImages(List<ApiMessageAndVersion> fromRecords) {
        List<ImageDeltaPair<?, ?>> testMatrix = Arrays.asList(
            new ImageDeltaPair<>(() -> AclsImage.EMPTY, AclsDelta::new),
            new ImageDeltaPair<>(() -> ClientQuotasImage.EMPTY, ClientQuotasDelta::new),
            new ImageDeltaPair<>(() -> ClusterImage.EMPTY, ClusterDelta::new),
            new ImageDeltaPair<>(() -> ConfigurationsImage.EMPTY, ConfigurationsDelta::new),
            new ImageDeltaPair<>(() -> DelegationTokenImage.EMPTY, DelegationTokenDelta::new),
            new ImageDeltaPair<>(() -> FeaturesImage.EMPTY, FeaturesDelta::new),
            new ImageDeltaPair<>(() -> ProducerIdsImage.EMPTY, ProducerIdsDelta::new),
            new ImageDeltaPair<>(() -> ScramImage.EMPTY, ScramDelta::new),
            new ImageDeltaPair<>(() -> TopicsImage.EMPTY, TopicsDelta::new)
        );

        // test from empty image stopping each of the various intermediate images along the way
        for (ImageDeltaPair<?, ?> pair : testMatrix) {
            new TestThroughAllIntermediateImagesLeadingToFinalImageHelper<>(
                (Supplier<Object>) pair.imageSupplier(), (Function<Object, Object>) pair.deltaCreator()
            ).test(fromRecords);
        }
    }

    @Test
    public void testActivationRecordsPartialTransaction() {
        OffsetControlManager offsetControlManager = new OffsetControlManager.Builder().build();
        offsetControlManager.replay(new BeginTransactionRecord(), 10);
        offsetControlManager.handleCommitBatch(Batch.data(20, 1, 1L, 0,
            Collections.singletonList(new ApiMessageAndVersion(new BeginTransactionRecord(), (short) 0))));

        ControllerResult<Void> result = ActivationRecordsGenerator.generate(
            logMsg -> { },
            false,
            offsetControlManager.transactionStartOffset(),
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_6_IV1, "test"),
            MetadataVersion.IBP_3_6_IV1);

        assertTrue(result.isAtomic());
        offsetControlManager.replay(
            RecordTestUtils.recordAtIndexAs(AbortTransactionRecord.class, result.records(), 0).get(),
            21
        );
        assertEquals(-1L, offsetControlManager.transactionStartOffset());
    }

    @Test
    public void testActivationRecordsPartialTransactionNoSupport() {
        OffsetControlManager offsetControlManager = new OffsetControlManager.Builder().build();
        offsetControlManager.replay(new BeginTransactionRecord(), 10);
        offsetControlManager.handleCommitBatch(Batch.data(20, 1, 1L, 0,
            Collections.singletonList(new ApiMessageAndVersion(new BeginTransactionRecord(), (short) 0))));

        assertThrows(RuntimeException.class, () ->
            ActivationRecordsGenerator.generate(
                msg -> { },
                false,
                offsetControlManager.transactionStartOffset(),
                BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_6_IV0, "test"),
                MetadataVersion.IBP_3_6_IV0)
        );
    }
}
