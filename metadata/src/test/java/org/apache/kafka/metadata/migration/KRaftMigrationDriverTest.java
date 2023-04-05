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
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.ConfigurationsImage;
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
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.image.TopicsImageTest.IMAGE1;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KRaftMigrationDriverTest {
    static class NoOpRecordConsumer implements ZkRecordConsumer {
        @Override
        public void beginMigration() {

        }

        @Override
        public CompletableFuture<?> acceptBatch(List<ApiMessageAndVersion> recordBatch) {
            System.err.println(recordBatch);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public OffsetAndEpoch completeMigration() {
            return new OffsetAndEpoch(100, 1);
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

        @Override
        public void setMetadataVersion(MetadataVersion metadataVersion) {

        }
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
    @Test
    public void testOnlySendNeededRPCsToBrokers() throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CapturingMigrationClient migrationClient = new CapturingMigrationClient(
            new HashSet<>(Arrays.asList(1, 2, 3)), new CapturingTopicMigrationClient());
        KRaftMigrationDriver driver = new KRaftMigrationDriver(
            3000,
            new NoOpRecordConsumer(),
            migrationClient,
            metadataPropagator,
            metadataPublisher -> { },
            new MockFaultHandler("test")
        );

        MetadataImage image = MetadataImage.EMPTY;
        MetadataDelta delta = new MetadataDelta(image);

        driver.start();
        delta.replay(zkBrokerRecord(1));
        delta.replay(zkBrokerRecord(2));
        delta.replay(zkBrokerRecord(3));
        MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
        image = delta.apply(provenance);

        // Publish a delta with this node (3000) as the leader
        LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
        driver.onControllerChange(newLeader);
        driver.onMetadataUpdate(delta, image, new LogDeltaManifest(provenance, newLeader, 1, 100, 42));

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

        assertEquals(1, migrationClient.capturedConfigs.size());
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

        driver.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMigrationWithClientException(boolean authException) throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CountDownLatch claimLeaderAttempts = new CountDownLatch(3);
        CapturingMigrationClient migrationClient = new CapturingMigrationClient(new HashSet<>(Arrays.asList(1, 2, 3)), new CapturingTopicMigrationClient()) {
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
        try (KRaftMigrationDriver driver = new KRaftMigrationDriver(
            3000,
            new NoOpRecordConsumer(),
            migrationClient,
            metadataPropagator,
            metadataPublisher -> { },
            faultHandler
        )) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);

            driver.start();
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Notify the driver that it is the leader
            driver.onControllerChange(new LeaderAndEpoch(OptionalInt.of(3000), 1));
            // Publish metadata of all the ZK brokers being ready
            driver.onMetadataUpdate(delta, image, new LogDeltaManifest(provenance,
                new LeaderAndEpoch(OptionalInt.of(3000), 1), 1, 100, 42));
            Assertions.assertTrue(claimLeaderAttempts.await(1, TimeUnit.MINUTES));
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.ZK_MIGRATION),
                "Waiting for KRaftMigrationDriver to enter ZK_MIGRATION state");

            if (authException) {
                assertEquals(MigrationClientAuthException.class, faultHandler.firstException().getCause().getClass());
            } else {
                Assertions.assertNull(faultHandler.firstException());
            }
        }
    }

    @Test
    public void testTopicSnapshot() throws Exception {
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
        CapturingMigrationClient migrationClient = new CapturingMigrationClient(new HashSet<>(Arrays.asList(1, 2, 3)), topicClient);

        try (KRaftMigrationDriver driver = new KRaftMigrationDriver(
                3000,
                new NoOpRecordConsumer(),
                migrationClient,
                metadataPropagator,
                metadataPublisher -> { },
                new MockFaultHandler("test")
        )) {

            MetadataImage image = new MetadataImage(
                MetadataProvenance.EMPTY,
                FeaturesImage.EMPTY,
                ClusterImage.EMPTY,
                IMAGE1,
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY);
            MetadataDelta delta = new MetadataDelta(image);

            driver.start();
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Publish a delta with this node (3000) as the leader
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);
            driver.onMetadataUpdate(delta, image, new LogDeltaManifest(provenance, newLeader, 1, 100, 42));

            // Wait for migration
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "Waiting for KRaftMigrationDriver to enter ZK_MIGRATION state");

            // Delete a topic in KRaft snapshot
            provenance = new MetadataProvenance(200, 1, 1);
            delta = new MetadataDelta(image);
            delta.replay(new RemoveTopicRecord().setTopicId(IMAGE1.getTopic("foo").id()));
            image = delta.apply(provenance);
            driver.onMetadataUpdate(delta, image, new SnapshotManifest(provenance, 100));
            driver.migrationState().get(1, TimeUnit.MINUTES);
            assertEquals(1, topicClient.deletedTopics.size());
            assertEquals("foo", topicClient.deletedTopics.get(0));
        }
    }
}