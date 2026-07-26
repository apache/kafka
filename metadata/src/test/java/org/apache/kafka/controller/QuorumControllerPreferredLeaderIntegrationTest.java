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
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.Listener;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.ListenerCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignmentCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.controller.ControllerRequestContextUtil.ANONYMOUS_CONTEXT;
import static org.apache.kafka.controller.QuorumControllerIntegrationTestUtils.brokerFeatures;
import static org.apache.kafka.controller.QuorumControllerIntegrationTestUtils.sendBrokerHeartbeatToUnfenceBrokers;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Integration tests for the gated preferred-leader election feature.
 * These tests exercise the full QuorumController stack, including the periodic
 * electPreferred task, wait-for-sync gating, round-robin scheduling, and JMX metrics.
 */
@Timeout(value = 40)
public class QuorumControllerPreferredLeaderIntegrationTest {

    static final BootstrapMetadata SIMPLE_BOOTSTRAP = BootstrapMetadata.
            fromVersion(MetadataVersion.IBP_3_7_IV0, "test-provided bootstrap");

    // Short check interval so tests complete quickly.
    static final long CHECK_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(100);
    static final long SESSION_TIMEOUT_MS = 1_000;

    /**
     * Verify that with algorithm=wait-for-sync and threshold=0, the controller does not
     * elect a recovering broker as preferred leader while any of its preferred partitions
     * are still out of ISR. Once all partitions rejoin the ISR, elections complete normally.
     */
    @Test
    public void testWaitForSyncGatesElectionUntilBrokerIsInSync() throws Throwable {
        int numPartitions = 5;

        try (
            MockRaftClientTestEnv clientEnv = new MockRaftClientTestEnv.Builder(1).build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(clientEnv)
                .setBootstrapMetadata(SIMPLE_BOOTSTRAP)
                .setSessionTimeoutMillis(OptionalLong.of(SESSION_TIMEOUT_MS))
                .setLeaderImbalanceCheckIntervalNs(OptionalLong.of(CHECK_INTERVAL_NS))
                .setControllerBuilderInitializer(b -> {
                    b.setLeaderImbalanceElectionAlgorithm("wait-for-sync");
                    b.setLeaderImbalanceElectionWaitForSyncThresholdPercent(0);
                    b.setLeaderImbalanceElectionWaitForSyncMaxWaitMs(0L); // no escape hatch
                })
                .build()
        ) {
            QuorumController active = controlEnv.activeController();
            ListenerCollection listeners = new ListenerCollection();
            listeners.add(new Listener().setName("PLAINTEXT").setHost("localhost").setPort(9092));

            // Register brokers 0, 1, 2 — but only unfence 1 and 2.
            // Broker 0 is preferred in the assignment but starts fenced.
            Map<Integer, Long> brokerEpochs = new HashMap<>();
            for (int brokerId : List.of(0, 1, 2)) {
                BrokerRegistrationReply reply = active.registerBroker(ANONYMOUS_CONTEXT,
                    new BrokerRegistrationRequestData()
                        .setBrokerId(brokerId)
                        .setClusterId(active.clusterId())
                        .setFeatures(brokerFeatures(MetadataVersion.MINIMUM_VERSION, MetadataVersion.IBP_3_7_IV0))
                        .setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB" + brokerId))
                        .setLogDirs(List.of(
                            Uuid.fromString("TESTBROKER" + Integer.toString(100000 + brokerId).substring(1) + "DIRAAAA")
                        ))
                        .setListeners(listeners)
                ).get();
                brokerEpochs.put(brokerId, reply.epoch());
            }
            // Unfence only brokers 1 and 2; broker 0 remains fenced.
            sendBrokerHeartbeatToUnfenceBrokers(active, List.of(1, 2), brokerEpochs);

            // Create 5 partitions all assigned as {0, 1, 2}: broker 0 is preferred but fenced,
            // so the leader falls to broker 1 — all partitions start imbalanced.
            Uuid topicId = createTopicWithManualAssignment(active, "test", numPartitions, List.of(0, 1, 2));
            assertTrue(active.replicationControl().arePartitionLeadersImbalanced());

            // Unfence broker 0 (sends heartbeat), but broker 0 is NOT yet in the ISR for any partition.
            active.processBrokerHeartbeat(ANONYMOUS_CONTEXT,
                new BrokerHeartbeatRequestData()
                    .setWantFence(false)
                    .setBrokerEpoch(brokerEpochs.get(0))
                    .setBrokerId(0)
                    .setCurrentMetadataOffset(100000L)
            ).get();

            // Wait until the electPreferred task has run and gated broker 0.
            // gatedPreferredLeaderBrokerCount() > 0 proves the balancer ran and found the gate.
            TestUtils.retryOnExceptionWithTimeout(30_000, () -> {
                assertTrue(active.controllerMetrics().gatedPreferredLeaderBrokerCount() > 0,
                    "Broker 0 should be gated (all 5 partitions out of ISR)");
            });

            // Gate is active — no elections should have happened.
            assertTrue(active.replicationControl().arePartitionLeadersImbalanced(),
                "Partitions should still be imbalanced while broker 0 is gated");

            // Add broker 0 to the ISR for all 5 partitions (simulates catch-up completion).
            for (int i = 0; i < numPartitions; i++) {
                reportBrokerInIsr(active, topicId, i, List.of(0, 1, 2), brokerEpochs);
            }

            // After 2 consecutive ungated runs (hysteresis), the gate is released and elections fire.
            TestUtils.waitForCondition(
                () -> !active.replicationControl().arePartitionLeadersImbalanced(),
                TimeUnit.NANOSECONDS.toMillis(CHECK_INTERVAL_NS) * 30,
                "Leaders should balance after broker 0 rejoins ISR"
            );

            // All 5 partitions should now have broker 0 as leader.
            for (int i = 0; i < numPartitions; i++) {
                assertEquals(0, active.replicationControl().getPartition(topicId, i).leader,
                    "Partition " + i + " should have broker 0 as leader");
            }

            // Gate metric should have cleared since broker 0 is now fully balanced.
            TestUtils.retryOnExceptionWithTimeout(5_000, () ->
                assertEquals(0L, active.controllerMetrics().gatedPreferredLeaderBrokerCount(),
                    "Gated broker count should be 0 after all partitions are balanced")
            );
        }
    }

    /**
     * Verify that with maxPerRun=2 and two brokers each recovering with 4 imbalanced
     * partitions, round-robin scheduling ensures both brokers make progress each run
     * and both eventually reach a fully balanced state.
     */
    @Test
    public void testRoundRobinDistributesElectionsAcrossRecoveringBrokers() throws Throwable {
        int partitionsPerBroker = 4;
        int maxPerRun = 2;

        try (
            MockRaftClientTestEnv clientEnv = new MockRaftClientTestEnv.Builder(1).build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(clientEnv)
                .setBootstrapMetadata(SIMPLE_BOOTSTRAP)
                .setSessionTimeoutMillis(OptionalLong.of(SESSION_TIMEOUT_MS))
                .setLeaderImbalanceCheckIntervalNs(OptionalLong.of(CHECK_INTERVAL_NS))
                .setControllerBuilderInitializer(b ->
                    b.setLeaderImbalanceElectionMaxPerRun(maxPerRun)
                )
                .build()
        ) {
            QuorumController active = controlEnv.activeController();
            ListenerCollection listeners = new ListenerCollection();
            listeners.add(new Listener().setName("PLAINTEXT").setHost("localhost").setPort(9092));

            // Register 4 brokers: 0 and 1 are preferred (start fenced); 2 and 3 are replicas.
            Map<Integer, Long> brokerEpochs = new HashMap<>();
            for (int brokerId : List.of(0, 1, 2, 3)) {
                BrokerRegistrationReply reply = active.registerBroker(ANONYMOUS_CONTEXT,
                    new BrokerRegistrationRequestData()
                        .setBrokerId(brokerId)
                        .setClusterId(active.clusterId())
                        .setFeatures(brokerFeatures(MetadataVersion.MINIMUM_VERSION, MetadataVersion.IBP_3_7_IV0))
                        .setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB" + brokerId))
                        .setLogDirs(List.of(
                            Uuid.fromString("TESTBROKER" + Integer.toString(100000 + brokerId).substring(1) + "DIRAAAA")
                        ))
                        .setListeners(listeners)
                ).get();
                brokerEpochs.put(brokerId, reply.epoch());
            }
            // Unfence only the replica brokers (2 and 3); preferred brokers 0 and 1 start fenced.
            sendBrokerHeartbeatToUnfenceBrokers(active, List.of(2, 3), brokerEpochs);

            // foo: 4 partitions preferred on broker 0 (assignments {0, 2, 3})
            // bar: 4 partitions preferred on broker 1 (assignments {1, 2, 3})
            Uuid fooId = createTopicWithManualAssignment(active, "foo", partitionsPerBroker, List.of(0, 2, 3));
            Uuid barId = createTopicWithManualAssignment(active, "bar", partitionsPerBroker, List.of(1, 2, 3));

            // Both topics fully imbalanced (preferred leaders fenced).
            assertTrue(active.replicationControl().arePartitionLeadersImbalanced());

            // Unfence brokers 0 and 1 (both recovering simultaneously).
            sendBrokerHeartbeatToUnfenceBrokers(active, List.of(0, 1), brokerEpochs);

            // Add broker 0 and broker 1 to the ISR for all their respective partitions.
            for (int i = 0; i < partitionsPerBroker; i++) {
                reportBrokerInIsr(active, fooId, i, List.of(0, 2, 3), brokerEpochs);
                reportBrokerInIsr(active, barId, i, List.of(1, 2, 3), brokerEpochs);
            }

            // With maxPerRun=2, each run elects at most 2 partitions. Round-robin ensures
            // both broker 0 and broker 1 receive elections in each capped run rather than
            // one broker being starved. Eventually all 8 partitions must be balanced.
            TestUtils.waitForCondition(
                () -> !active.replicationControl().arePartitionLeadersImbalanced(),
                TimeUnit.NANOSECONDS.toMillis(CHECK_INTERVAL_NS) * 30,
                "Both foo and bar topics should be fully balanced via round-robin"
            );

            // Verify each partition's leader is the intended preferred broker.
            for (int i = 0; i < partitionsPerBroker; i++) {
                assertEquals(0, active.replicationControl().getPartition(fooId, i).leader,
                    "foo partition " + i + " should have broker 0 as leader");
                assertEquals(1, active.replicationControl().getPartition(barId, i).leader,
                    "bar partition " + i + " should have broker 1 as leader");
            }
        }
    }

    /**
     * Verify that when a broker stays OOS beyond maxWaitMs, the escape hatch fires:
     * elections proceed for partitions where the broker IS in the ISR, and
     * PreferredLeaderElectionEscapeHatchCount increments.
     */
    @Test
    public void testEscapeHatchElectsInSyncPartitionsAfterMaxWaitMs() throws Throwable {
        // Use a longer maxWaitMs than the check interval so gating is observed first.
        long maxWaitMs = 300L;

        try (
            MockRaftClientTestEnv clientEnv = new MockRaftClientTestEnv.Builder(1).build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(clientEnv)
                .setBootstrapMetadata(SIMPLE_BOOTSTRAP)
                .setSessionTimeoutMillis(OptionalLong.of(SESSION_TIMEOUT_MS))
                .setLeaderImbalanceCheckIntervalNs(OptionalLong.of(CHECK_INTERVAL_NS))
                .setControllerBuilderInitializer(b -> {
                    b.setLeaderImbalanceElectionAlgorithm("wait-for-sync");
                    b.setLeaderImbalanceElectionWaitForSyncThresholdPercent(0);
                    b.setLeaderImbalanceElectionWaitForSyncMaxWaitMs(maxWaitMs);
                })
                .build()
        ) {
            QuorumController active = controlEnv.activeController();
            ListenerCollection listeners = new ListenerCollection();
            listeners.add(new Listener().setName("PLAINTEXT").setHost("localhost").setPort(9092));

            Map<Integer, Long> brokerEpochs = new HashMap<>();
            for (int brokerId : List.of(0, 1, 2)) {
                BrokerRegistrationReply reply = active.registerBroker(ANONYMOUS_CONTEXT,
                    new BrokerRegistrationRequestData()
                        .setBrokerId(brokerId)
                        .setClusterId(active.clusterId())
                        .setFeatures(brokerFeatures(MetadataVersion.MINIMUM_VERSION, MetadataVersion.IBP_3_7_IV0))
                        .setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB" + brokerId))
                        .setLogDirs(List.of(
                            Uuid.fromString("TESTBROKER" + Integer.toString(100000 + brokerId).substring(1) + "DIRAAAA")
                        ))
                        .setListeners(listeners)
                ).get();
                brokerEpochs.put(brokerId, reply.epoch());
            }
            sendBrokerHeartbeatToUnfenceBrokers(active, List.of(1, 2), brokerEpochs);

            // 2 partitions both preferred on broker 0; leaders fall to broker 1 (broker 0 fenced).
            Uuid topicId = createTopicWithManualAssignment(active, "test", 2, List.of(0, 1, 2));

            // Unfence broker 0; add it to ISR for p0 only — p1 stays OOS.
            active.processBrokerHeartbeat(ANONYMOUS_CONTEXT,
                new BrokerHeartbeatRequestData()
                    .setWantFence(false)
                    .setBrokerEpoch(brokerEpochs.get(0))
                    .setBrokerId(0)
                    .setCurrentMetadataOffset(100000L)
            ).get();
            reportBrokerInIsr(active, topicId, 0, List.of(0, 1, 2), brokerEpochs);
            // p1 intentionally left OOS.

            TestUtils.retryOnExceptionWithTimeout(30_000, () ->
                assertTrue(active.controllerMetrics().gatedPreferredLeaderBrokerCount() > 0,
                    "Broker 0 should be gated while p1 is OOS")
            );

            // Wait for the escape hatch to fire after maxWaitMs elapses.
            TestUtils.retryOnExceptionWithTimeout(30_000, () ->
                assertTrue(active.controllerMetrics().preferredLeaderElectionEscapeHatchCount() > 0,
                    "Escape hatch should have fired after maxWaitMs elapsed")
            );

            assertEquals(0, active.replicationControl().getPartition(topicId, 0).leader,
                "p0 should have broker 0 as leader after escape hatch");
            assertTrue(active.replicationControl().getPartition(topicId, 1).leader != 0,
                "p1 should not have broker 0 as leader (still OOS)");
        }
    }

    /**
     * Verify the OutOfSyncPreferredPartitionCount data pipeline end-to-end: the periodic
     * task must populate brokerOutOfSyncCounts() correctly as brokers recover, and the
     * map must be empty once all brokers are fully in sync.
     */
    @Test
    public void testBrokerOutOfSyncCountsWiredThroughPeriodicTask() throws Throwable {
        try (
            MockRaftClientTestEnv clientEnv = new MockRaftClientTestEnv.Builder(1).build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(clientEnv)
                .setBootstrapMetadata(SIMPLE_BOOTSTRAP)
                .setSessionTimeoutMillis(OptionalLong.of(SESSION_TIMEOUT_MS))
                .setLeaderImbalanceCheckIntervalNs(OptionalLong.of(CHECK_INTERVAL_NS))
                .setControllerBuilderInitializer(b -> {
                    b.setLeaderImbalanceElectionAlgorithm("wait-for-sync");
                    b.setLeaderImbalanceElectionWaitForSyncThresholdPercent(0);
                    b.setLeaderImbalanceElectionWaitForSyncMaxWaitMs(0L);
                })
                .build()
        ) {
            QuorumController active = controlEnv.activeController();
            ListenerCollection listeners = new ListenerCollection();
            listeners.add(new Listener().setName("PLAINTEXT").setHost("localhost").setPort(9092));

            // Register brokers 0–3: 0 and 1 are preferred, 2 and 3 are replicas.
            Map<Integer, Long> brokerEpochs = new HashMap<>();
            for (int brokerId : List.of(0, 1, 2, 3)) {
                BrokerRegistrationReply reply = active.registerBroker(ANONYMOUS_CONTEXT,
                    new BrokerRegistrationRequestData()
                        .setBrokerId(brokerId)
                        .setClusterId(active.clusterId())
                        .setFeatures(brokerFeatures(MetadataVersion.MINIMUM_VERSION, MetadataVersion.IBP_3_7_IV0))
                        .setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB" + brokerId))
                        .setLogDirs(List.of(
                            Uuid.fromString("TESTBROKER" + Integer.toString(100000 + brokerId).substring(1) + "DIRAAAA")
                        ))
                        .setListeners(listeners)
                ).get();
                brokerEpochs.put(brokerId, reply.epoch());
            }
            sendBrokerHeartbeatToUnfenceBrokers(active, List.of(2, 3), brokerEpochs);

            // foo: 3 partitions preferred on broker 0 ({0,2,3})
            // bar: 2 partitions preferred on broker 1 ({1,2,3})
            Uuid fooId = createTopicWithManualAssignment(active, "foo", 3, List.of(0, 2, 3));
            Uuid barId = createTopicWithManualAssignment(active, "bar", 2, List.of(1, 2, 3));

            // Unfence preferred brokers; broker 0 joins ISR for foo-p0 only (2 OOS).
            // Broker 1 joins no ISRs (2 OOS).
            sendBrokerHeartbeatToUnfenceBrokers(active, List.of(0, 1), brokerEpochs);
            reportBrokerInIsr(active, fooId, 0, List.of(0, 2, 3), brokerEpochs);

            // broker 0: 2 OOS (foo-p1, foo-p2); broker 1: 2 OOS (all bar partitions).
            TestUtils.retryOnExceptionWithTimeout(30_000, () ->
                assertEquals(Map.of(0, 2, 1, 2),
                    active.replicationControl().brokerOutOfSyncCounts(),
                    "broker 0 has 2 OOS, broker 1 has 2 OOS")
            );

            // Broker 0 catches up for foo-p1 and foo-p2.
            reportBrokerInIsr(active, fooId, 1, List.of(0, 2, 3), brokerEpochs);
            reportBrokerInIsr(active, fooId, 2, List.of(0, 2, 3), brokerEpochs);

            // Broker 0 fully in sync — must be absent from the map.
            TestUtils.retryOnExceptionWithTimeout(30_000, () ->
                assertEquals(Map.of(1, 2),
                    active.replicationControl().brokerOutOfSyncCounts(),
                    "broker 0 absent (in sync), broker 1 still has 2 OOS")
            );

            // Broker 1 catches up for both bar partitions.
            reportBrokerInIsr(active, barId, 0, List.of(1, 2, 3), brokerEpochs);
            reportBrokerInIsr(active, barId, 1, List.of(1, 2, 3), brokerEpochs);

            // All in sync — map must be empty after hysteresis clears.
            TestUtils.retryOnExceptionWithTimeout(30_000, () ->
                assertEquals(Map.of(),
                    active.replicationControl().brokerOutOfSyncCounts(),
                    "all brokers in sync, OOS map must be empty")
            );
        }
    }

    /**
     * Create a topic with a fixed manual replica assignment applied to every partition.
     * Returns the topic ID.
     */
    private static Uuid createTopicWithManualAssignment(
        QuorumController active,
        String topicName,
        int numPartitions,
        List<Integer> replicaAssignment
    ) throws Exception {
        CreatableReplicaAssignmentCollection assignments = new CreatableReplicaAssignmentCollection();
        for (int i = 0; i < numPartitions; i++) {
            assignments.add(new CreatableReplicaAssignment()
                .setPartitionIndex(i)
                .setBrokerIds(replicaAssignment));
        }
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic()
            .setName(topicName)
            .setNumPartitions(-1)
            .setReplicationFactor((short) -1)
            .setAssignments(assignments));
        CreateTopicsResponseData response = active.createTopics(
            ANONYMOUS_CONTEXT, request, Set.of(topicName), false).get();
        assertEquals(Errors.NONE, Errors.forCode(response.topics().find(topicName).errorCode()));
        return response.topics().find(topicName).topicId();
    }

    /**
     * Send an AlterPartition request to the controller on behalf of the current partition
     * leader, reporting {@code newIsr} as the new in-sync replica set.
     */
    private static void reportBrokerInIsr(
        QuorumController active,
        Uuid topicId,
        int partitionIndex,
        List<Integer> newIsr,
        Map<Integer, Long> brokerEpochs
    ) throws Exception {
        PartitionRegistration pr = active.replicationControl().getPartition(topicId, partitionIndex);
        AlterPartitionRequestData.PartitionData partitionData = new AlterPartitionRequestData.PartitionData()
            .setPartitionIndex(partitionIndex)
            .setLeaderEpoch(pr.leaderEpoch)
            .setPartitionEpoch(pr.partitionEpoch)
            .setNewIsrWithEpochs(AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(newIsr));
        AlterPartitionRequestData.TopicData topicData = new AlterPartitionRequestData.TopicData()
            .setTopicId(topicId);
        topicData.partitions().add(partitionData);
        AlterPartitionRequestData request = new AlterPartitionRequestData()
            .setBrokerId(pr.leader)
            .setBrokerEpoch(brokerEpochs.get(pr.leader));
        request.topics().add(topicData);
        active.alterPartition(ANONYMOUS_CONTEXT,
            new AlterPartitionRequest.Builder(request)
                .build(ApiKeys.ALTER_PARTITION.oldestVersion()).data()).get();
    }
}
