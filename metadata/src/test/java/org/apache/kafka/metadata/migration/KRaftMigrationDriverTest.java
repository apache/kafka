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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class KRaftMigrationDriverTest {
    class NoOpRecordConsumer implements ZkRecordConsumer {
        @Override
        public void beginMigration() {

        }

        @Override
        public CompletableFuture<?> acceptBatch(List<ApiMessageAndVersion> recordBatch) {
            return null;
        }

        @Override
        public OffsetAndEpoch completeMigration() {
            return new OffsetAndEpoch(100, 1);
        }

        @Override
        public void abortMigration() {

        }
    }

    class CapturingMigrationClient implements MigrationClient {

        private final Set<Integer> brokerIds;
        public final Map<ConfigResource, Map<String, String>> capturedConfigs = new HashMap<>();

        public CapturingMigrationClient(Set<Integer> brokerIdsInZk) {
            this.brokerIds = brokerIdsInZk;
        }

        @Override
        public ZkMigrationLeadershipState getOrCreateMigrationRecoveryState(ZkMigrationLeadershipState initialState) {
            return initialState;
        }

        @Override
        public ZkMigrationLeadershipState setMigrationRecoveryState(ZkMigrationLeadershipState state) {
            return state;
        }

        @Override
        public ZkMigrationLeadershipState claimControllerLeadership(ZkMigrationLeadershipState state) {
            return state;
        }

        @Override
        public ZkMigrationLeadershipState releaseControllerLeadership(ZkMigrationLeadershipState state) {
            return state;
        }

        @Override
        public ZkMigrationLeadershipState createTopic(String topicName, Uuid topicId, Map<Integer, PartitionRegistration> topicPartitions, ZkMigrationLeadershipState state) {
            return state;
        }

        @Override
        public ZkMigrationLeadershipState updateTopicPartitions(Map<String, Map<Integer, PartitionRegistration>> topicPartitions, ZkMigrationLeadershipState state) {
            return state;
        }

        @Override
        public ZkMigrationLeadershipState writeConfigs(ConfigResource configResource, Map<String, String> configMap, ZkMigrationLeadershipState state) {
            capturedConfigs.computeIfAbsent(configResource, __ -> new HashMap<>()).putAll(configMap);
            return state;
        }

        @Override
        public ZkMigrationLeadershipState writeClientQuotas(Map<String, String> clientQuotaEntity, Map<String, Double> quotas, ZkMigrationLeadershipState state) {
            return state;
        }

        @Override
        public ZkMigrationLeadershipState writeProducerId(long nextProducerId, ZkMigrationLeadershipState state) {
            return state;
        }

        @Override
        public void readAllMetadata(Consumer<List<ApiMessageAndVersion>> batchConsumer, Consumer<Integer> brokerIdConsumer) {

        }

        @Override
        public Set<Integer> readBrokerIds() {
            return brokerIds;
        }

        @Override
        public Set<Integer> readBrokerIdsFromTopicAssignments() {
            return brokerIds;
        }

        @Override
        public ZkMigrationLeadershipState writeMetadataDeltaToZookeeper(MetadataDelta delta, MetadataImage image, ZkMigrationLeadershipState state) {
            return state;
        }
    }

    class CountingMetadataPropagator implements LegacyPropagator {

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
        public void sendRPCsToBrokersFromMetadataDelta(MetadataDelta delta, MetadataImage image, int zkControllerEpoch) {
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

    @Test
    public void testOnlySendNeededRPCsToBrokers() throws Exception {
        // KAFKA-14668 Don't send RPCs to brokers for every metadata change, only when broker or topics change.
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CapturingMigrationClient migrationClient = new CapturingMigrationClient(new HashSet<>(Arrays.asList(1, 2, 3)));
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
        driver.publishLogDelta(delta, image, new LogDeltaManifest(provenance, new LeaderAndEpoch(OptionalInt.of(3000), 1), 1, 100, 42));
        TestUtils.waitForCondition(() -> driver.migrationState().get(10, TimeUnit.SECONDS).equals(MigrationState.DUAL_WRITE),
            "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");

        Assertions.assertEquals(1, metadataPropagator.images);
        Assertions.assertEquals(0, metadataPropagator.deltas);

        delta = new MetadataDelta(image);
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("1").setName("foo").setValue("bar"));
        provenance = new MetadataProvenance(120, 1, 2);
        image = delta.apply(provenance);
        driver.enqueueMetadataChangeEvent(delta, image, provenance, false).get(10, TimeUnit.SECONDS);

        Assertions.assertEquals(1, migrationClient.capturedConfigs.size());
        Assertions.assertEquals(1, metadataPropagator.images);
        Assertions.assertEquals(0, metadataPropagator.deltas);

        delta = new MetadataDelta(image);
        delta.replay(new BrokerRegistrationChangeRecord()
            .setBrokerId(1)
            .setBrokerEpoch(0)
            .setFenced(BrokerRegistrationFencingChange.NONE.value())
            .setInControlledShutdown(BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.value()));
        provenance = new MetadataProvenance(130, 1, 3);
        image = delta.apply(provenance);
        driver.enqueueMetadataChangeEvent(delta, image, provenance, false).get(10, TimeUnit.SECONDS);

        Assertions.assertEquals(1, metadataPropagator.images);
        Assertions.assertEquals(1, metadataPropagator.deltas);

        driver.close();
    }
}
