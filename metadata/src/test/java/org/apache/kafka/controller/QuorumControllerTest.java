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

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.BrokerIdNotRegisteredException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.Listener;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.ListenerCollection;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignmentCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpointCollection;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.controller.QuorumController.ConfigResourceExistenceChecker;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.util.BatchFileWriter;
import org.apache.kafka.metalog.LocalLogManager;
import org.apache.kafka.metalog.LocalLogManagerTestEnv;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.snapshot.FileRawSnapshotReader;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.Snapshots;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.function.Function.identity;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.BROKER0;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.SCHEMA;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.entry;
import static org.apache.kafka.controller.ControllerRequestContextUtil.ANONYMOUS_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class QuorumControllerTest {
    private final static Logger log = LoggerFactory.getLogger(QuorumControllerTest.class);

    static final BootstrapMetadata SIMPLE_BOOTSTRAP = BootstrapMetadata.
            fromVersion(MetadataVersion.IBP_3_3_IV3, "test-provided bootstrap");

    /**
     * Test creating a new QuorumController and closing it.
     */
    @Test
    public void testCreateAndClose() throws Throwable {
        MockControllerMetrics metrics = new MockControllerMetrics();
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setMetrics(metrics);
                }).
                build()
        ) {
        }
        assertTrue(metrics.isClosed(), "metrics were not closed");
    }

    /**
     * Test setting some configuration values and reading them back.
     */
    @Test
    public void testConfigurationOperations() throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setConfigSchema(SCHEMA);
                }).
                build();
        ) {
            controlEnv.activeController().registerBroker(ANONYMOUS_CONTEXT,
                new BrokerRegistrationRequestData().
                setFeatures(brokerFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_4_IV1)).
                setBrokerId(0).
                setClusterId(logEnv.clusterId())).get();
            testConfigurationOperations(controlEnv.activeController());
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
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setConfigSchema(SCHEMA);
                }).
                build();
        ) {
            controlEnv.activeController().registerBroker(ANONYMOUS_CONTEXT,
                new BrokerRegistrationRequestData().
                    setFeatures(brokerFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_4_IV1)).
                    setBrokerId(0).
                    setClusterId(logEnv.clusterId())).get();
            testDelayedConfigurationOperations(logEnv, controlEnv.activeController());
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
        logEnv.logManagers().forEach(m -> m.setMaxReadOffset(3L));
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
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setConfigSchema(SCHEMA);
                }).
                setSessionTimeoutMillis(OptionalLong.of(sessionTimeoutMillis)).
                setBootstrapMetadata(SIMPLE_BOOTSTRAP).
                build();
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
                        setFeatures(brokerFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_3_IV3)).
                        setIncarnationId(Uuid.randomUuid()).
                        setListeners(listeners));
                brokerEpochs.put(brokerId, reply.get().epoch());
            }

            // Brokers are only registered and should still be fenced
            allBrokers.forEach(brokerId -> {
                assertFalse(active.clusterControl().isUnfenced(brokerId),
                    "Broker " + brokerId + " should have been fenced");
            });

            // Unfence all brokers and create a topic foo
            sendBrokerheartbeat(active, allBrokers, brokerEpochs);
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
                    sendBrokerheartbeat(active, brokersToKeepUnfenced, brokerEpochs);
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
            sendBrokerheartbeat(active, brokersToKeepUnfenced, brokerEpochs);

            // At this point only the brokers we want fenced should be fenced.
            brokersToKeepUnfenced.forEach(brokerId -> {
                assertTrue(active.clusterControl().isUnfenced(brokerId),
                    "Broker " + brokerId + " should have been unfenced");
            });
            brokersToFence.forEach(brokerId -> {
                assertFalse(active.clusterControl().isUnfenced(brokerId),
                    "Broker " + brokerId + " should have been fenced");
            });

            // Verify the isr and leaders for the topic partition
            int[] expectedIsr = {1};
            int[] isrFoo = active.replicationControl().getPartition(topicIdFoo, 0).isr;

            assertTrue(Arrays.equals(isrFoo, expectedIsr),
                "The ISR for topic foo was " + Arrays.toString(isrFoo) +
                    ". It is expected to be " + Arrays.toString(expectedIsr));

            int fooLeader = active.replicationControl().getPartition(topicIdFoo, 0).leader;
            assertEquals(expectedIsr[0], fooLeader);

            // Check that there are imbalaned partitions
            assertTrue(active.replicationControl().arePartitionLeadersImbalanced());
        }
    }

    @Test
    public void testBalancePartitionLeaders() throws Throwable {
        List<Integer> allBrokers = Arrays.asList(1, 2, 3);
        List<Integer> brokersToKeepUnfenced = Arrays.asList(1, 2);
        List<Integer> brokersToFence = Arrays.asList(3);
        short replicationFactor = (short) allBrokers.size();
        short numberOfPartitions = (short) allBrokers.size();
        long sessionTimeoutMillis = 1000;
        long leaderImbalanceCheckIntervalNs = 1_000_000_000;

        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setConfigSchema(SCHEMA);
                }).
                setSessionTimeoutMillis(OptionalLong.of(sessionTimeoutMillis)).
                setLeaderImbalanceCheckIntervalNs(OptionalLong.of(leaderImbalanceCheckIntervalNs)).
                setBootstrapMetadata(SIMPLE_BOOTSTRAP).
                build();
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
                        setFeatures(brokerFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_3_IV3)).
                        setIncarnationId(Uuid.randomUuid()).
                        setListeners(listeners));
                brokerEpochs.put(brokerId, reply.get().epoch());
            }

            // Brokers are only registered and should still be fenced
            allBrokers.forEach(brokerId -> {
                assertFalse(active.clusterControl().isUnfenced(brokerId),
                    "Broker " + brokerId + " should have been fenced");
            });

            // Unfence all brokers and create a topic foo
            sendBrokerheartbeat(active, allBrokers, brokerEpochs);
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
                    sendBrokerheartbeat(active, brokersToKeepUnfenced, brokerEpochs);
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
            sendBrokerheartbeat(active, brokersToKeepUnfenced, brokerEpochs);

            // At this point only the brokers we want fenced should be fenced.
            brokersToKeepUnfenced.forEach(brokerId -> {
                assertTrue(active.clusterControl().isUnfenced(brokerId),
                    "Broker " + brokerId + " should have been unfenced");
            });
            brokersToFence.forEach(brokerId -> {
                assertFalse(active.clusterControl().isUnfenced(brokerId),
                    "Broker " + brokerId + " should have been fenced");
            });

            // Check that there are imbalaned partitions
            assertTrue(active.replicationControl().arePartitionLeadersImbalanced());

            // Re-register all fenced brokers
            for (Integer brokerId : brokersToFence) {
                CompletableFuture<BrokerRegistrationReply> reply = active.registerBroker(
                    ANONYMOUS_CONTEXT,
                    new BrokerRegistrationRequestData().
                        setBrokerId(brokerId).
                        setClusterId(active.clusterId()).
                        setFeatures(brokerFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_3_IV3)).
                        setIncarnationId(Uuid.randomUuid()).
                        setListeners(listeners));
                brokerEpochs.put(brokerId, reply.get().epoch());
            }

            // Unfence all brokers
            sendBrokerheartbeat(active, allBrokers, brokerEpochs);

            // Let the unfenced broker, 3, join the ISR partition 2
            Set<TopicIdPartition> imbalancedPartitions = active.replicationControl().imbalancedPartitions();
            assertEquals(1, imbalancedPartitions.size());
            int imbalancedPartitionId = imbalancedPartitions.iterator().next().partitionId();
            PartitionRegistration partitionRegistration = active.replicationControl().getPartition(topicIdFoo, imbalancedPartitionId);
            AlterPartitionRequestData.PartitionData partitionData = new AlterPartitionRequestData.PartitionData()
                .setPartitionIndex(imbalancedPartitionId)
                .setLeaderEpoch(partitionRegistration.leaderEpoch)
                .setPartitionEpoch(partitionRegistration.partitionEpoch)
                .setNewIsr(Arrays.asList(1, 2, 3));

            AlterPartitionRequestData.TopicData topicData = new AlterPartitionRequestData.TopicData()
                .setTopicName("foo");
            topicData.partitions().add(partitionData);

            AlterPartitionRequestData alterPartitionRequest = new AlterPartitionRequestData()
                .setBrokerId(partitionRegistration.leader)
                .setBrokerEpoch(brokerEpochs.get(partitionRegistration.leader));
            alterPartitionRequest.topics().add(topicData);

            active.alterPartition(ANONYMOUS_CONTEXT, alterPartitionRequest).get();

            // Check that partitions are balanced
            AtomicLong lastHeartbeat = new AtomicLong(active.time().milliseconds());
            TestUtils.waitForCondition(
                () -> {
                    if (active.time().milliseconds() > lastHeartbeat.get() + (sessionTimeoutMillis / 2)) {
                        lastHeartbeat.set(active.time().milliseconds());
                        sendBrokerheartbeat(active, allBrokers, brokerEpochs);
                    }
                    return !active.replicationControl().arePartitionLeadersImbalanced();
                },
                TimeUnit.MILLISECONDS.convert(leaderImbalanceCheckIntervalNs * 10, TimeUnit.NANOSECONDS),
                "Leaders where not balanced after unfencing all of the brokers"
            );
        }
    }

    @Test
    public void testNoOpRecordWriteAfterTimeout() throws Throwable {
        long maxIdleIntervalNs = 1_000;
        long maxReplicationDelayMs = 60_000;
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setConfigSchema(SCHEMA);
                    controllerBuilder.setMaxIdleIntervalNs(OptionalLong.of(maxIdleIntervalNs));
                }).
                build();
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

    @Test
    public void testUnregisterBroker() throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setConfigSchema(SCHEMA);
                }).
                build();
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
                    setFeatures(brokerFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_4_IV1)).
                    setListeners(listeners));
            assertEquals(2L, reply.get().epoch());
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
                        setWantFence(false).setBrokerEpoch(2L).setBrokerId(0).
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
        }
    }

    private BrokerRegistrationRequestData.FeatureCollection brokerFeatures() {
        return brokerFeatures(MetadataVersion.MINIMUM_KRAFT_VERSION, MetadataVersion.latest());
    }

    private BrokerRegistrationRequestData.FeatureCollection brokerFeatures(
        MetadataVersion minVersion,
        MetadataVersion maxVersion
    ) {
        BrokerRegistrationRequestData.FeatureCollection features = new BrokerRegistrationRequestData.FeatureCollection();
        features.add(new BrokerRegistrationRequestData.Feature()
            .setName(MetadataVersion.FEATURE_NAME)
            .setMinSupportedVersion(minVersion.featureLevel())
            .setMaxSupportedVersion(maxVersion.featureLevel()));
        return features;
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
        RawSnapshotReader reader = null;
        Uuid fooId;
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setConfigSchema(SCHEMA);
                }).
                setBootstrapMetadata(SIMPLE_BOOTSTRAP).
                build();
        ) {
            QuorumController active = controlEnv.activeController();
            for (int i = 0; i < numBrokers; i++) {
                BrokerRegistrationReply reply = active.registerBroker(ANONYMOUS_CONTEXT,
                    new BrokerRegistrationRequestData().
                        setBrokerId(i).
                        setRack(null).
                        setClusterId(active.clusterId()).
                        setFeatures(brokerFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_3_IV3)).
                        setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB" + i)).
                        setListeners(new ListenerCollection(Arrays.asList(new Listener().
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
        }
    }

    private List<ApiMessageAndVersion> generateTestRecords(Uuid fooId, Map<Integer, Long> brokerEpochs) {
        return Arrays.asList(
            new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(MetadataVersion.IBP_3_3_IV3.featureLevel()), (short) 0),
            new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerId(0).setBrokerEpoch(brokerEpochs.get(0)).
                setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB0")).
                setEndPoints(new BrokerEndpointCollection(
                    Arrays.asList(new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").
                        setPort(9092).setSecurityProtocol((short) 0)).iterator())).
                setFeatures(registrationFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_3_IV3)).
                setRack(null).
                setFenced(true), (short) 1),
            new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerId(1).setBrokerEpoch(brokerEpochs.get(1)).
                setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB1")).
                setEndPoints(new BrokerEndpointCollection(Arrays.asList(
                    new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").
                        setPort(9093).setSecurityProtocol((short) 0)).iterator())).
                setFeatures(registrationFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_3_IV3)).
                setRack(null).
                setFenced(true), (short) 1),
            new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerId(2).setBrokerEpoch(brokerEpochs.get(2)).
                setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB2")).
                setEndPoints(new BrokerEndpointCollection(
                    Arrays.asList(new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").
                        setPort(9094).setSecurityProtocol((short) 0)).iterator())).
                setFeatures(registrationFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_3_IV3)).
                setRack(null).
                setFenced(true), (short) 1),
            new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerId(3).setBrokerEpoch(brokerEpochs.get(3)).
                setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB3")).
                setEndPoints(new BrokerEndpointCollection(Arrays.asList(
                    new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").
                        setPort(9095).setSecurityProtocol((short) 0)).iterator())).
                setFeatures(registrationFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_3_IV3)).
                setRack(null).
                setFenced(true), (short) 1),
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
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setConfigSchema(SCHEMA);
                }).
                build();
        ) {
            QuorumController controller = controlEnv.activeController();
            CountDownLatch countDownLatch = controller.pause();
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
        }
    }

    private static void assertYieldsTimeout(Future<?> future) {
        assertEquals(TimeoutException.class, assertThrows(ExecutionException.class,
            () -> future.get()).getCause().getClass());
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
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setConfigSchema(SCHEMA);
                }).
                build();
        ) {
            QuorumController controller = controlEnv.activeController();
            CountDownLatch countDownLatch = controller.pause();
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
        }
    }

    @Disabled // TODO: need to fix leader election in LocalLog.
    @Test
    public void testMissingInMemorySnapshot() throws Exception {
        int numBrokers = 3;
        int numPartitions = 3;
        String topicName = "topic-name";

        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setConfigSchema(SCHEMA);
                }).
                build();
        ) {
            QuorumController controller = controlEnv.activeController();

            Map<Integer, Long> brokerEpochs = registerBrokers(controller, numBrokers);

            // Create a lot of partitions
            List<CreatableReplicaAssignment> partitions = IntStream
                .range(0, numPartitions)
                .mapToObj(partitionIndex -> new CreatableReplicaAssignment()
                    .setPartitionIndex(partitionIndex)
                    .setBrokerIds(Arrays.asList(0, 1, 2))
                )
                .collect(Collectors.toList());

            Uuid topicId = controller.createTopics(ANONYMOUS_CONTEXT, new CreateTopicsRequestData()
                    .setTopics(new CreatableTopicCollection(Collections.singleton(new CreatableTopic()
                        .setName(topicName)
                        .setNumPartitions(-1)
                        .setReplicationFactor((short) -1)
                        .setAssignments(new CreatableReplicaAssignmentCollection(partitions.iterator()))
                    ).iterator())),
                Collections.singleton("foo")).get().topics().find(topicName).topicId();

            // Create a lot of alter isr
            List<AlterPartitionRequestData.PartitionData> alterPartitions = IntStream
                .range(0, numPartitions)
                .mapToObj(partitionIndex -> {
                    PartitionRegistration partitionRegistration = controller.replicationControl().getPartition(
                        topicId,
                        partitionIndex
                    );

                    return new AlterPartitionRequestData.PartitionData()
                        .setPartitionIndex(partitionIndex)
                        .setLeaderEpoch(partitionRegistration.leaderEpoch)
                        .setPartitionEpoch(partitionRegistration.partitionEpoch)
                        .setNewIsr(Arrays.asList(0, 1));
                })
                .collect(Collectors.toList());

            AlterPartitionRequestData.TopicData topicData = new AlterPartitionRequestData.TopicData()
                .setTopicName(topicName);
            topicData.partitions().addAll(alterPartitions);

            int leaderId = 0;
            AlterPartitionRequestData alterPartitionRequest = new AlterPartitionRequestData()
                .setBrokerId(leaderId)
                .setBrokerEpoch(brokerEpochs.get(leaderId));
            alterPartitionRequest.topics().add(topicData);

            logEnv.logManagers().get(0).resignAfterNonAtomicCommit();

            int oldClaimEpoch = controller.curClaimEpoch();
            assertThrows(ExecutionException.class,
                () -> controller.alterPartition(ANONYMOUS_CONTEXT, alterPartitionRequest).get());

            // Wait for the controller to become active again
            assertSame(controller, controlEnv.activeController());
            assertTrue(
                oldClaimEpoch < controller.curClaimEpoch(),
                String.format("oldClaimEpoch = %s, newClaimEpoch = %s", oldClaimEpoch, controller.curClaimEpoch())
            );

            // Since the alterPartition partially failed we expect to see
            // some partitions to still have 2 in the ISR.
            int partitionsWithReplica2 = Utils.toList(
                controller
                    .replicationControl()
                    .brokersToIsrs()
                    .partitionsWithBrokerInIsr(2)
            ).size();
            int partitionsWithReplica0 = Utils.toList(
                controller
                    .replicationControl()
                    .brokersToIsrs()
                    .partitionsWithBrokerInIsr(0)
            ).size();

            assertEquals(numPartitions, partitionsWithReplica0);
            assertNotEquals(0, partitionsWithReplica2);
            assertTrue(
                partitionsWithReplica0 > partitionsWithReplica2,
                String.format(
                    "partitionsWithReplica0 = %s, partitionsWithReplica2 = %s",
                    partitionsWithReplica0,
                    partitionsWithReplica2
                )
            );
        }
    }

    private Map<Integer, Long> registerBrokers(QuorumController controller, int numBrokers) throws Exception {
        Map<Integer, Long> brokerEpochs = new HashMap<>();
        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            BrokerRegistrationReply reply = controller.registerBroker(ANONYMOUS_CONTEXT,
                new BrokerRegistrationRequestData()
                    .setBrokerId(brokerId)
                    .setRack(null)
                    .setClusterId(controller.clusterId())
                    .setFeatures(brokerFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.IBP_3_4_IV1))
                    .setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB" + brokerId))
                    .setListeners(
                        new ListenerCollection(
                            Arrays.asList(
                                new Listener()
                                .setName("PLAINTEXT")
                                .setHost("localhost")
                                .setPort(9092 + brokerId)
                                ).iterator()
                            )
                        )
                    ).get();
            brokerEpochs.put(brokerId, reply.epoch());

            // Send heartbeat to unfence
            controller.processBrokerHeartbeat(ANONYMOUS_CONTEXT,
                new BrokerHeartbeatRequestData()
                    .setWantFence(false)
                    .setBrokerEpoch(brokerEpochs.get(brokerId))
                    .setBrokerId(brokerId)
                    .setCurrentMetadataOffset(100000L)
            ).get();
        }

        return brokerEpochs;
    }

    private void sendBrokerheartbeat(
        QuorumController controller,
        List<Integer> brokers,
        Map<Integer, Long> brokerEpochs
    ) throws Exception {
        if (brokers.isEmpty()) {
            return;
        }
        for (Integer brokerId : brokers) {
            BrokerHeartbeatReply reply = controller.processBrokerHeartbeat(ANONYMOUS_CONTEXT,
                new BrokerHeartbeatRequestData()
                    .setWantFence(false)
                    .setBrokerEpoch(brokerEpochs.get(brokerId))
                    .setBrokerId(brokerId)
                    .setCurrentMetadataOffset(100000)
            ).get();
            assertEquals(new BrokerHeartbeatReply(true, false, false, false), reply);
        }
    }

    @Test
    public void testConfigResourceExistenceChecker() throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setConfigSchema(SCHEMA);
                }).
                build();
        ) {
            QuorumController active = controlEnv.activeController();
            registerBrokers(active, 5);
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
        }
    }

    private static final Uuid FOO_ID = Uuid.fromString("igRktLOnR8ektWHr79F8mw");

    private static final Map<Integer, Long> ALL_ZERO_BROKER_EPOCHS =
        IntStream.of(0, 1, 2, 3).boxed().collect(Collectors.toMap(identity(), __ -> 0L));

    @Test
    public void testQuorumControllerCompletesAuthorizerInitialLoad() throws Throwable {
        final int numControllers = 3;
        List<StandardAuthorizer> authorizers = new ArrayList<>(numControllers);
        for (int i = 0; i < numControllers; i++) {
            StandardAuthorizer authorizer = new StandardAuthorizer();
            authorizer.configure(Collections.emptyMap());
            authorizers.add(authorizer);
        }
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(numControllers).
                setSharedLogDataInitializer(sharedLogData -> {
                    sharedLogData.setInitialMaxReadOffset(2);
                }).
                build()
        ) {
            logEnv.appendInitialRecords(generateTestRecords(FOO_ID, ALL_ZERO_BROKER_EPOCHS));
            logEnv.logManagers().forEach(m -> m.setMaxReadOffset(2));
            try (
                QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                    setControllerBuilderInitializer(controllerBuilder -> {
                        controllerBuilder.setAuthorizer(authorizers.get(controllerBuilder.nodeId()));
                    }).
                    build()
            ) {
                assertInitialLoadFuturesNotComplete(authorizers);
                logEnv.logManagers().get(0).setMaxReadOffset(Long.MAX_VALUE);
                QuorumController active = controlEnv.activeController();
                active.unregisterBroker(ANONYMOUS_CONTEXT, 3).get();
                assertInitialLoadFuturesNotComplete(authorizers.stream().skip(1).collect(Collectors.toList()));
                logEnv.logManagers().forEach(m -> m.setMaxReadOffset(Long.MAX_VALUE));
                TestUtils.waitForCondition(() -> {
                    return authorizers.stream().allMatch(a -> a.initialLoadFuture().isDone());
                }, "Failed to complete initial authorizer load for all controllers.");
            }
        }
    }

    @Test
    public void testFatalMetadataReplayErrorOnActive() throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                build();
        ) {
            QuorumController active = controlEnv.activeController();
            CompletableFuture<Void> future = active.appendWriteEvent("errorEvent",
                    OptionalLong.empty(), () -> {
                        return ControllerResult.of(Collections.singletonList(new ApiMessageAndVersion(
                                new ConfigRecord().
                                        setName(null).
                                        setResourceName(null).
                                        setResourceType((byte) 255).
                                        setValue(null), (short) 0)), null);
                    });
            assertThrows(ExecutionException.class, () -> future.get());
            assertEquals(NullPointerException.class, controlEnv.fatalFaultHandler(active.nodeId())
                .firstException().getCause().getClass());
            controlEnv.ignoreFatalFaults();
        }
    }

    @Test
    public void testFatalMetadataErrorDuringSnapshotLoading() throws Exception {
        InitialSnapshot invalidSnapshot = new InitialSnapshot(Collections.unmodifiableList(Arrays.asList(
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0)))
        );

        LocalLogManagerTestEnv.Builder logEnvBuilder = new LocalLogManagerTestEnv.Builder(3)
            .setSnapshotReader(FileRawSnapshotReader.open(
                invalidSnapshot.tempDir.toPath(),
                new OffsetAndEpoch(0, 0)
            ));

        try (LocalLogManagerTestEnv logEnv = logEnvBuilder.build()) {
            try (QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).build()) {
                TestUtils.waitForCondition(() -> controlEnv.controllers().stream().allMatch(controller -> {
                    return controlEnv.fatalFaultHandler(controller.nodeId()).firstException() != null;
                }),
                    "At least one controller failed to detect the fatal fault"
                );
                controlEnv.ignoreFatalFaults();
            }
        }
    }

    @Test
    public void testFatalMetadataErrorDuringLogLoading() throws Exception {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).build()) {
            logEnv.appendInitialRecords(Collections.unmodifiableList(Arrays.asList(
                new ApiMessageAndVersion(new PartitionRecord(), (short) 0))
            ));

            try (QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).build()) {
                TestUtils.waitForCondition(() -> controlEnv.controllers().stream().allMatch(controller -> {
                    return controlEnv.fatalFaultHandler(controller.nodeId()).firstException() != null;
                }),
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
        File tempDir = null;
        BatchFileWriter writer = null;

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

    private final static List<ApiMessageAndVersion> PRE_PRODUCTION_RECORDS =
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

    private final static BootstrapMetadata COMPLEX_BOOTSTRAP = BootstrapMetadata.fromRecords(
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
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setConfigSchema(SCHEMA);
                }).
                setBootstrapMetadata(COMPLEX_BOOTSTRAP).
                build();
        ) {
            QuorumController active = controlEnv.activeController();
            TestUtils.waitForCondition(() ->
                active.featureControl().metadataVersion().equals(MetadataVersion.IBP_3_0_IV1),
                "Failed to get a metadata version of " + MetadataVersion.IBP_3_0_IV1);
            // The ConfigRecord in our bootstrap should not have been applied, since there
            // were already records present.
            assertEquals(Collections.emptyMap(), active.configurationControl().
                    getConfigs(new ConfigResource(BROKER, "")));
        }
    }

    @Test
    public void testInsertBootstrapRecordsToEmptyLog() throws Exception {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setControllerBuilderInitializer(controllerBuilder -> {
                    controllerBuilder.setConfigSchema(SCHEMA);
                }).
                setBootstrapMetadata(COMPLEX_BOOTSTRAP).
                build();
        ) {
            QuorumController active = controlEnv.activeController();

            ControllerRequestContext ctx = new ControllerRequestContext(
                new RequestHeaderData(), KafkaPrincipal.ANONYMOUS, OptionalLong.of(Long.MAX_VALUE));

            TestUtils.waitForCondition(() -> {
                FinalizedControllerFeatures features = active.finalizedFeatures(ctx).get();
                Optional<Short> metadataVersionOpt = features.get(MetadataVersion.FEATURE_NAME);
                return Optional.of(MetadataVersion.IBP_3_3_IV1.featureLevel()).equals(metadataVersionOpt);
            }, "Failed to see expected metadata version from bootstrap metadata");

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
}
