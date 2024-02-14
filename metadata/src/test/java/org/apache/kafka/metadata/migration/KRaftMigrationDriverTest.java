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
package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.QuorumFeatures;
import org.apache.kafka.controller.metrics.QuorumControllerMetrics;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.DelegationTokenImage;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.ProducerIdsImage;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.image.loader.SnapshotManifest;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.kafka.image.TopicsImageTest.DELTA1_RECORDS;
import static org.apache.kafka.image.TopicsImageTest.IMAGE1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KRaftMigrationDriverTest {
    private final static QuorumFeatures QUORUM_FEATURES = new QuorumFeatures(4,
        QuorumFeatures.defaultFeatureMap(true),
        Arrays.asList(4, 5, 6));

    static class MockControllerMetrics extends QuorumControllerMetrics {
        final AtomicBoolean closed = new AtomicBoolean(false);

        MockControllerMetrics() {
            super(Optional.empty(), Time.SYSTEM, false);
        }

        @Override
        public void close() {
            super.close();
            closed.set(true);
        }
    }
    MockControllerMetrics metrics = new MockControllerMetrics();

    Time mockTime = new MockTime(1) {
        public long nanoseconds() {
            // We poll the event for each 1 sec, make it happen for each 10 ms to speed up the test
            return System.nanoTime() - NANOSECONDS.convert(990, MILLISECONDS);
        }
    };

    /**
     * Return a {@link org.apache.kafka.metadata.migration.KRaftMigrationDriver.Builder} that uses the mocks
     * defined in this class.
     */
    KRaftMigrationDriver.Builder defaultTestBuilder() {
        return KRaftMigrationDriver.newBuilder()
            .setNodeId(3000)
            .setZkRecordConsumer(new NoOpRecordConsumer())
            .setInitialZkLoadHandler(metadataPublisher -> { })
            .setFaultHandler(new MockFaultHandler("test"))
            .setQuorumFeatures(QUORUM_FEATURES)
            .setConfigSchema(KafkaConfigSchema.EMPTY)
            .setControllerMetrics(metrics)
            .setTime(mockTime);
    }

    static class NoOpRecordConsumer implements ZkRecordConsumer {
        @Override
        public CompletableFuture<?> beginMigration() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<?> acceptBatch(List<ApiMessageAndVersion> recordBatch) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<OffsetAndEpoch> completeMigration() {
            return CompletableFuture.completedFuture(new OffsetAndEpoch(100, 1));
        }

        @Override
        public void abortMigration() {

        }
    }

    static class CountingMetadataPropagator implements LegacyPropagator {

        public int deltas = 0;
        public int images = 0;

        @Override
        public void startup() {

        }

        @Override
        public void shutdown() {

        }

        @Override
        public void publishMetadata(MetadataImage image) {

        }

        @Override
        public void sendRPCsToBrokersFromMetadataDelta(
            MetadataDelta delta,
            MetadataImage image,
            int zkControllerEpoch
        ) {
            deltas += 1;
        }

        @Override
        public void sendRPCsToBrokersFromMetadataImage(MetadataImage image, int zkControllerEpoch) {
            images += 1;
        }

        @Override
        public void clear() {

        }
    }

    static LogDeltaManifest.Builder logDeltaManifestBuilder(MetadataProvenance provenance, LeaderAndEpoch newLeader) {
        return LogDeltaManifest.newBuilder()
            .provenance(provenance)
            .leaderAndEpoch(newLeader)
            .numBatches(1)
            .elapsedNs(100)
            .numBytes(42);
    }

    RegisterBrokerRecord zkBrokerRecord(int id) {
        RegisterBrokerRecord record = new RegisterBrokerRecord();
        record.setBrokerId(id);
        record.setIsMigratingZkBroker(true);
        record.setFenced(false);
        return record;
    }

    /**
     * Enqueues a metadata change event with the migration driver and returns a future that can be waited on in
     * the test code. The future will complete once the metadata change event executes completely.
     */
    CompletableFuture<Void> enqueueMetadataChangeEventWithFuture(
        KRaftMigrationDriver driver,
        MetadataDelta delta,
        MetadataImage newImage,
        MetadataProvenance provenance
    ) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Consumer<Throwable> completionHandler = ex -> {
            if (ex == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(ex);
            }
        };

        driver.enqueueMetadataChangeEvent(delta, newImage, provenance, false, completionHandler);
        return future;
    }

    /**
     * Don't send RPCs to brokers for every metadata change, only when brokers or topics change.
     * This is a regression test for KAFKA-14668
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOnlySendNeededRPCsToBrokers(boolean registerControllers) throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(1, 2, 3)
            .setConfigMigrationClient(configClient)
            .build();
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
            .setZkMigrationClient(migrationClient)
            .setPropagator(metadataPropagator)
            .setInitialZkLoadHandler(metadataPublisher -> { });

        try (KRaftMigrationDriver driver = builder.build()) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);

            driver.start();
            setupDeltaForMigration(delta, registerControllers);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Publish a delta with this node (3000) as the leader
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");

            assertEquals(1, metadataPropagator.images);
            assertEquals(0, metadataPropagator.deltas);

            delta = new MetadataDelta(image);
            delta.replay(new ConfigRecord()
                .setResourceType(ConfigResource.Type.BROKER.id())
                .setResourceName("1")
                .setName("foo")
                .setValue("bar"));
            provenance = new MetadataProvenance(120, 1, 2);
            image = delta.apply(provenance);
            enqueueMetadataChangeEventWithFuture(driver, delta, image, provenance).get(1, TimeUnit.MINUTES);

            assertEquals(1, configClient.writtenConfigs.size());
            assertEquals(1, metadataPropagator.images);
            assertEquals(0, metadataPropagator.deltas);

            delta = new MetadataDelta(image);
            delta.replay(new BrokerRegistrationChangeRecord()
                .setBrokerId(1)
                .setBrokerEpoch(0)
                .setFenced(BrokerRegistrationFencingChange.NONE.value())
                .setInControlledShutdown(BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.value()));
            provenance = new MetadataProvenance(130, 1, 3);
            image = delta.apply(provenance);
            enqueueMetadataChangeEventWithFuture(driver, delta, image, provenance).get(1, TimeUnit.MINUTES);

            assertEquals(1, metadataPropagator.images);
            assertEquals(1, metadataPropagator.deltas);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMigrationWithClientException(boolean authException) throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CountDownLatch claimLeaderAttempts = new CountDownLatch(3);
        CapturingMigrationClient migrationClient = new CapturingMigrationClient(new HashSet<>(Arrays.asList(1, 2, 3)),
                new CapturingTopicMigrationClient(),
                new CapturingConfigMigrationClient(),
                new CapturingAclMigrationClient(),
                new CapturingDelegationTokenMigrationClient(),
                CapturingMigrationClient.EMPTY_BATCH_SUPPLIER) {
            @Override
            public ZkMigrationLeadershipState claimControllerLeadership(ZkMigrationLeadershipState state) {
                if (claimLeaderAttempts.getCount() == 0) {
                    return super.claimControllerLeadership(state);
                } else {
                    claimLeaderAttempts.countDown();
                    if (authException) {
                        throw new MigrationClientAuthException(new RuntimeException("Some kind of ZK auth error!"));
                    } else {
                        throw new MigrationClientException("Some kind of ZK error!");
                    }
                }

            }
        };
        MockFaultHandler faultHandler = new MockFaultHandler("testMigrationClientExpiration");
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
            .setZkMigrationClient(migrationClient)
            .setFaultHandler(faultHandler)
            .setPropagator(metadataPropagator);
        try (KRaftMigrationDriver driver = builder.build()) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);
            setupDeltaForMigration(delta, true);

            driver.start();
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Notify the driver that it is the leader
            driver.onControllerChange(new LeaderAndEpoch(OptionalInt.of(3000), 1));
            // Publish metadata of all the ZK brokers being ready
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance,
                new LeaderAndEpoch(OptionalInt.of(3000), 1)).build());
            assertTrue(claimLeaderAttempts.await(1, TimeUnit.MINUTES));
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");

            if (authException) {
                assertEquals(MigrationClientAuthException.class, faultHandler.firstException().getCause().getClass());
            } else {
                Assertions.assertNull(faultHandler.firstException());
            }
        }
    }

    private void setupDeltaForMigration(
        MetadataDelta delta,
        boolean registerControllers
    ) {
        if (registerControllers) {
            delta.replay(new FeatureLevelRecord().
                    setName(MetadataVersion.FEATURE_NAME).
                    setFeatureLevel(MetadataVersion.IBP_3_7_IV0.featureLevel()));
            for (int id : QUORUM_FEATURES.quorumNodeIds()) {
                delta.replay(RecordTestUtils.createTestControllerRegistration(id, true));
            }
        } else {
            delta.replay(new FeatureLevelRecord().
                    setName(MetadataVersion.FEATURE_NAME).
                    setFeatureLevel(MetadataVersion.IBP_3_6_IV2.featureLevel()));
        }
    }

    private void setupDeltaWithControllerRegistrations(
        MetadataDelta delta,
        List<Integer> notReadyIds,
        List<Integer> readyIds
    ) {
        delta.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(MetadataVersion.IBP_3_7_IV0.featureLevel()));
        delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
        for (int id : notReadyIds) {
            delta.replay(RecordTestUtils.createTestControllerRegistration(id, false));
        }
        for (int id : readyIds) {
            delta.replay(RecordTestUtils.createTestControllerRegistration(id, true));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testShouldNotMoveToNextStateIfControllerNodesAreNotReadyToMigrate(
        boolean allNodePresent
    ) throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder().setBrokersInZk(1).build();

        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
            .setZkMigrationClient(migrationClient)
            .setPropagator(metadataPropagator);
        try (KRaftMigrationDriver driver = builder.build()) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);

            driver.start();
            if (allNodePresent) {
                setupDeltaWithControllerRegistrations(delta, Arrays.asList(4, 5, 6), Arrays.asList());
            } else {
                setupDeltaWithControllerRegistrations(delta, Arrays.asList(), Arrays.asList(4, 5));
            }
            delta.replay(zkBrokerRecord(1));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Publish a delta with this node (3000) as the leader
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            // Not all controller nodes are ready. So we should stay at WAIT_FOR_CONTROLLER_QUORUM state.
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.WAIT_FOR_CONTROLLER_QUORUM),
                "Waiting for KRaftMigrationDriver to enter WAIT_FOR_CONTROLLER_QUORUM state");

            // Controller nodes don't have zkMigrationReady set. Should still stay at WAIT_FOR_CONTROLLER_QUORUM state.
            assertEquals(MigrationDriverState.WAIT_FOR_CONTROLLER_QUORUM, driver.migrationState().get(1, TimeUnit.MINUTES));

            // Update so that all controller nodes are zkMigrationReady. Now we should be able to move to the next state.
            delta = new MetadataDelta(image);
            setupDeltaWithControllerRegistrations(delta, Arrays.asList(), Arrays.asList(4, 5, 6));
            image = delta.apply(new MetadataProvenance(200, 1, 2));
            driver.onMetadataUpdate(delta, image, new LogDeltaManifest.Builder().
                    provenance(image.provenance()).
                    leaderAndEpoch(newLeader).
                    numBatches(1).
                    elapsedNs(100).
                    numBytes(42).
                    build());
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");
        }
    }

    @Test
    public void testSkipWaitForBrokersInDualWrite() throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CapturingMigrationClient.newBuilder().build();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder().build();
        MockFaultHandler faultHandler = new MockFaultHandler("testMigrationClientExpiration");
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
            .setZkMigrationClient(migrationClient)
            .setPropagator(metadataPropagator)
            .setFaultHandler(faultHandler);
        try (KRaftMigrationDriver driver = builder.build()) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);

            // Fake a complete migration with ZK client
            migrationClient.setMigrationRecoveryState(
                ZkMigrationLeadershipState.EMPTY.withKRaftMetadataOffsetAndEpoch(100, 1));

            driver.start();
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            delta.replay(ZkMigrationState.MIGRATION.toRecord().message());
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            driver.onControllerChange(new LeaderAndEpoch(OptionalInt.of(3000), 1));
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance,
                new LeaderAndEpoch(OptionalInt.of(3000), 1)).build());

            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "Waiting for KRaftMigrationDriver to enter ZK_MIGRATION state");
        }
    }

    @FunctionalInterface
    interface TopicDualWriteVerifier {
        void verify(
            KRaftMigrationDriver driver,
            CapturingMigrationClient migrationClient,
            CapturingTopicMigrationClient topicClient,
            CapturingConfigMigrationClient configClient
        ) throws Exception;
    }

    public void setupTopicDualWrite(TopicDualWriteVerifier verifier) throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();

        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient() {
            @Override
            public void iterateTopics(EnumSet<TopicVisitorInterest> interests, TopicVisitor visitor) {
                IMAGE1.topicsByName().forEach((topicName, topicImage) -> {
                    Map<Integer, List<Integer>> assignment = new HashMap<>();
                    topicImage.partitions().forEach((partitionId, partitionRegistration) ->
                        assignment.put(partitionId, IntStream.of(partitionRegistration.replicas).boxed().collect(Collectors.toList()))
                    );
                    visitor.visitTopic(topicName, topicImage.id(), assignment);

                    topicImage.partitions().forEach((partitionId, partitionRegistration) ->
                        visitor.visitPartition(new TopicIdPartition(topicImage.id(), new TopicPartition(topicName, partitionId)), partitionRegistration)
                    );
                });
            }
        };
        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0, 1, 2, 3, 4, 5)
            .setTopicMigrationClient(topicClient)
            .setConfigMigrationClient(configClient)
            .build();
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
            .setZkMigrationClient(migrationClient)
            .setPropagator(metadataPropagator);
        try (KRaftMigrationDriver driver = builder.build()) {
            verifier.verify(driver, migrationClient, topicClient, configClient);
        }
    }

    @Test
    public void testTopicDualWriteSnapshot() throws Exception {
        setupTopicDualWrite((driver, migrationClient, topicClient, configClient) -> {
            MetadataImage image = new MetadataImage(
                MetadataProvenance.EMPTY,
                FeaturesImage.EMPTY,
                ClusterImage.EMPTY,
                IMAGE1,
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY,
                DelegationTokenImage.EMPTY);
            MetadataDelta delta = new MetadataDelta(image);

            driver.start();
            setupDeltaForMigration(delta, true);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(0));
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            delta.replay(zkBrokerRecord(4));
            delta.replay(zkBrokerRecord(5));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Publish a delta with this node (3000) as the leader
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            // Wait for migration
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "Waiting for KRaftMigrationDriver to enter ZK_MIGRATION state");

            // Modify topics in a KRaft snapshot -- delete foo, modify bar, add baz
            provenance = new MetadataProvenance(200, 1, 1);
            delta = new MetadataDelta(image);
            RecordTestUtils.replayAll(delta, DELTA1_RECORDS);
            image = delta.apply(provenance);
            driver.onMetadataUpdate(delta, image, new SnapshotManifest(provenance, 100));
            driver.migrationState().get(1, TimeUnit.MINUTES);

            assertEquals(1, topicClient.deletedTopics.size());
            assertEquals("foo", topicClient.deletedTopics.get(0));
            assertEquals(1, topicClient.createdTopics.size());
            assertEquals("baz", topicClient.createdTopics.get(0));
            assertTrue(topicClient.updatedTopicPartitions.get("bar").contains(0));
            assertEquals(new ConfigResource(ConfigResource.Type.TOPIC, "foo"), configClient.deletedResources.get(0));
        });
    }

    @Test
    public void testTopicDualWriteDelta() throws Exception {
        setupTopicDualWrite((driver, migrationClient, topicClient, configClient) -> {
            MetadataImage image = new MetadataImage(
                MetadataProvenance.EMPTY,
                FeaturesImage.EMPTY,
                ClusterImage.EMPTY,
                IMAGE1,
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY,
                DelegationTokenImage.EMPTY);
            MetadataDelta delta = new MetadataDelta(image);

            driver.start();
            setupDeltaForMigration(delta, true);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(0));
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            delta.replay(zkBrokerRecord(4));
            delta.replay(zkBrokerRecord(5));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Publish a delta with this node (3000) as the leader
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            // Wait for migration
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                    "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");

            // Modify topics in a KRaft snapshot -- delete foo, modify bar, add baz
            provenance = new MetadataProvenance(200, 1, 1);
            delta = new MetadataDelta(image);
            RecordTestUtils.replayAll(delta, DELTA1_RECORDS);
            image = delta.apply(provenance);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());
            driver.migrationState().get(1, TimeUnit.MINUTES);

            assertEquals(1, topicClient.deletedTopics.size());
            assertEquals("foo", topicClient.deletedTopics.get(0));
            assertEquals(1, topicClient.createdTopics.size());
            assertEquals("baz", topicClient.createdTopics.get(0));
            assertTrue(topicClient.updatedTopicPartitions.get("bar").contains(0));
            assertEquals(new ConfigResource(ConfigResource.Type.TOPIC, "foo"), configClient.deletedResources.get(0));
        });
    }

    @Test
    public void testNoDualWriteBeforeMigration() throws Exception {
        setupTopicDualWrite((driver, migrationClient, topicClient, configClient) -> {
            MetadataImage image = new MetadataImage(
                MetadataProvenance.EMPTY,
                FeaturesImage.EMPTY,
                ClusterImage.EMPTY,
                IMAGE1,
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY,
                DelegationTokenImage.EMPTY);
            MetadataDelta delta = new MetadataDelta(image);

            driver.start();
            setupDeltaForMigration(delta, true);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(0));
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            delta.replay(zkBrokerRecord(4));
            delta.replay(zkBrokerRecord(5));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Publish a delta with this node (3000) as the leader
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);

            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.WAIT_FOR_CONTROLLER_QUORUM),
                "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");

            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            driver.transitionTo(MigrationDriverState.WAIT_FOR_BROKERS);
            driver.transitionTo(MigrationDriverState.BECOME_CONTROLLER);
            driver.transitionTo(MigrationDriverState.ZK_MIGRATION);
            driver.transitionTo(MigrationDriverState.SYNC_KRAFT_TO_ZK);

            provenance = new MetadataProvenance(200, 1, 1);
            delta = new MetadataDelta(image);
            RecordTestUtils.replayAll(delta, DELTA1_RECORDS);
            image = delta.apply(provenance);
            driver.onMetadataUpdate(delta, image, new SnapshotManifest(provenance, 100));


            // Wait for migration
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");
        });
    }

    @Test
    public void testControllerFailover() throws Exception {
        setupTopicDualWrite((driver, migrationClient, topicClient, configClient) -> {
            MetadataImage image = new MetadataImage(
                MetadataProvenance.EMPTY,
                FeaturesImage.EMPTY,
                ClusterImage.EMPTY,
                IMAGE1,
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY,
                DelegationTokenImage.EMPTY);
            MetadataDelta delta = new MetadataDelta(image);

            driver.start();
            setupDeltaForMigration(delta, true);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(0));
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            delta.replay(zkBrokerRecord(4));
            delta.replay(zkBrokerRecord(5));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Publish a delta making a different node the leader
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3001), 1);
            driver.onControllerChange(newLeader);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            // Fake a complete migration
            migrationClient.setMigrationRecoveryState(
                ZkMigrationLeadershipState.EMPTY.withKRaftMetadataOffsetAndEpoch(100, 1));

            // Modify topics in a KRaft -- delete foo, modify bar, add baz
            provenance = new MetadataProvenance(200, 1, 1);
            delta = new MetadataDelta(image);
            RecordTestUtils.replayAll(delta, DELTA1_RECORDS);
            image = delta.apply(provenance);

            // Standby driver does not do anything with this delta besides remember the image
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            // Standby becomes leader
            newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "");
            assertEquals(1, topicClient.deletedTopics.size());
            assertEquals("foo", topicClient.deletedTopics.get(0));
            assertEquals(1, topicClient.createdTopics.size());
            assertEquals("baz", topicClient.createdTopics.get(0));
            assertTrue(topicClient.updatedTopicPartitions.get("bar").contains(0));
            assertEquals(new ConfigResource(ConfigResource.Type.TOPIC, "foo"), configClient.deletedResources.get(0));
        });
    }

    @Test
    public void testBeginMigrationOnce() throws Exception {
        AtomicInteger migrationBeginCalls = new AtomicInteger(0);
        NoOpRecordConsumer recordConsumer = new NoOpRecordConsumer() {
            @Override
            public CompletableFuture<?> beginMigration() {
                migrationBeginCalls.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
        };
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder().setBrokersInZk(1, 2, 3).build();
        MockFaultHandler faultHandler = new MockFaultHandler("testBeginMigrationOnce");
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
            .setZkMigrationClient(migrationClient)
            .setZkRecordConsumer(recordConsumer)
            .setPropagator(metadataPropagator)
            .setFaultHandler(faultHandler);
        try (KRaftMigrationDriver driver = builder.build()) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);

            driver.start();
            setupDeltaForMigration(delta, true);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            driver.onControllerChange(new LeaderAndEpoch(OptionalInt.of(3000), 1));
            
            // Call onMetadataUpdate twice. The first call will trigger the migration to begin (due to presence of brokers)
            // Both calls will "wakeup" the driver and cause a PollEvent to be run. Calling these back-to-back effectively
            // causes two MigrateMetadataEvents to be enqueued. Ensure only one is actually run.
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance,
                new LeaderAndEpoch(OptionalInt.of(3000), 1)).build());
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance,
                new LeaderAndEpoch(OptionalInt.of(3000), 1)).build());

            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                    "Waiting for KRaftMigrationDriver to enter ZK_MIGRATION state");
            assertEquals(1, migrationBeginCalls.get());
        }
    }

    private List<ApiMessageAndVersion> fillBatch(int size) {
        ApiMessageAndVersion[] batch = new ApiMessageAndVersion[size];
        Arrays.fill(batch, new ApiMessageAndVersion(new TopicRecord().setName("topic-fill").setTopicId(Uuid.randomUuid()), (short) 0));
        return Arrays.asList(batch);
    }

    static Stream<Arguments> batchSizes() {
        int defaultBatchSize = 200;
        return Stream.of(
            Arguments.of(Optional.of(defaultBatchSize), Arrays.asList(0, 0, 0, 0), 0, 0),
            Arguments.of(Optional.of(defaultBatchSize), Arrays.asList(0, 0, 1, 0), 1, 1),
            Arguments.of(Optional.of(defaultBatchSize), Arrays.asList(1, 1, 1, 1), 1, 4),
            Arguments.of(Optional.of(1000), Collections.singletonList(999), 1, 999),
            Arguments.of(Optional.of(1000), Collections.singletonList(1000), 1, 1000),
            Arguments.of(Optional.of(1000), Collections.singletonList(1001), 1, 1001),
            Arguments.of(Optional.of(1000), Arrays.asList(1000, 1), 2, 1001),
            Arguments.of(Optional.of(defaultBatchSize), Arrays.asList(0, 0, 0, 0), 0, 0),
            Arguments.of(Optional.of(1000), Arrays.asList(1000, 1000, 1000), 3, 3000),
            Arguments.of(Optional.of(defaultBatchSize), Collections.singletonList(defaultBatchSize + 1), 1, 201),
            Arguments.of(Optional.of(defaultBatchSize), Arrays.asList(defaultBatchSize, 1), 2, 201),
            Arguments.of(Optional.empty(), Collections.singletonList(defaultBatchSize + 1), 1, 201),
            Arguments.of(Optional.empty(), Arrays.asList(defaultBatchSize, 1), 2, 201)
        );
    }
    @ParameterizedTest
    @MethodSource("batchSizes")
    public void testCoalesceMigrationRecords(Optional<Integer> configBatchSize, List<Integer> batchSizes, int expectedBatchCount, int expectedRecordCount) throws Exception {
        List<List<ApiMessageAndVersion>> batchesPassedToController = new ArrayList<>();
        NoOpRecordConsumer recordConsumer = new NoOpRecordConsumer() {
            @Override
            public CompletableFuture<?> acceptBatch(List<ApiMessageAndVersion> recordBatch) {
                batchesPassedToController.add(recordBatch);
                return CompletableFuture.completedFuture(null);
            }
        };
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(1, 2, 3)
            .setBatchSupplier(new CapturingMigrationClient.MigrationBatchSupplier() {
                @Override
                public List<List<ApiMessageAndVersion>> recordBatches() {
                    List<List<ApiMessageAndVersion>> batches = new ArrayList<>();
                    for (int batchSize : batchSizes) {
                        batches.add(fillBatch(batchSize));
                    }
                    return batches;
                }
            })
            .build();
        MockFaultHandler faultHandler = new MockFaultHandler("testRebatchMigrationRecords");

        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
                .setZkMigrationClient(migrationClient)
                .setZkRecordConsumer(recordConsumer)
                .setPropagator(metadataPropagator)
                .setFaultHandler(faultHandler);
        configBatchSize.ifPresent(builder::setMinMigrationBatchSize);
        try (KRaftMigrationDriver driver = builder.build()) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);

            driver.start();
            setupDeltaForMigration(delta, true);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            driver.onControllerChange(new LeaderAndEpoch(OptionalInt.of(3000), 1));

            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance,
                    new LeaderAndEpoch(OptionalInt.of(3000), 1)).build());
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance,
                    new LeaderAndEpoch(OptionalInt.of(3000), 1)).build());

            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                    "Waiting for KRaftMigrationDriver to enter ZK_MIGRATION state");

            assertEquals(expectedBatchCount, batchesPassedToController.size());
            assertEquals(expectedRecordCount, batchesPassedToController.stream().mapToInt(List::size).sum());
        }
    }
}
