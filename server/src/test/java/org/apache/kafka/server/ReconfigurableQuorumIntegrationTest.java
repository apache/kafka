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

package org.apache.kafka.server;

import org.apache.kafka.clients.admin.AddRaftVoterOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.RaftVoterEndpoint;
import org.apache.kafka.clients.admin.RemoveRaftVoterOptions;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InconsistentClusterIdException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.test.api.TestKitDefaults;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.common.KRaftVersion;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.apache.kafka.test.TestUtils.retryOnExceptionWithTimeout;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class ReconfigurableQuorumIntegrationTest {
    private static void checkKRaftVersions(Admin admin, short finalized) throws Exception {
        FeatureMetadata featureMetadata = admin.describeFeatures().featureMetadata().get();
        if (finalized > 0) {
            assertTrue(featureMetadata.finalizedFeatures().containsKey(KRaftVersion.FEATURE_NAME),
                "finalizedFeatures does not contain " + KRaftVersion.FEATURE_NAME + ", finalizedFeatures: " + featureMetadata.finalizedFeatures());
            assertEquals(finalized, featureMetadata.finalizedFeatures().
                    get(KRaftVersion.FEATURE_NAME).minVersionLevel());
            assertEquals(finalized, featureMetadata.finalizedFeatures().
                    get(KRaftVersion.FEATURE_NAME).maxVersionLevel());
        } else {
            assertFalse(featureMetadata.finalizedFeatures().containsKey(KRaftVersion.FEATURE_NAME));
        }
        assertEquals((short) 0, featureMetadata.supportedFeatures().
                get(KRaftVersion.FEATURE_NAME).minVersion());
        assertEquals((short) 1, featureMetadata.supportedFeatures().
                get(KRaftVersion.FEATURE_NAME).maxVersion());
    }

    @Test
    public void testCreateAndDestroyNonReconfigurableCluster() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder().
                setNumBrokerNodes(1).
                setNumControllerNodes(1).
                build()
        ).build()) {
            cluster.format();
            cluster.startup();
            try (var admin = cluster.admin()) {
                retryOnExceptionWithTimeout(30_000, () ->
                    checkKRaftVersions(admin, KRaftVersion.KRAFT_VERSION_0.featureLevel()));
            }
        }
    }

    @Test
    public void testCreateAndDestroyReconfigurableCluster() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder().
                setNumBrokerNodes(1).
                setNumControllerNodes(1).
                build()
        ).setStandalone(true).build()) {
            cluster.format();
            cluster.startup();
            try (var admin = cluster.admin()) {
                retryOnExceptionWithTimeout(30_000, () ->
                    checkKRaftVersions(admin, KRaftVersion.KRAFT_VERSION_1.featureLevel()));
            }
        }
    }

    private static Map<Integer, Uuid> findVoterDirs(Admin admin) throws Exception {
        QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
        Map<Integer, Uuid> result = new TreeMap<>();
        quorumInfo.voters().forEach(v -> result.put(v.replicaId(), v.replicaDirectoryId()));
        return result;
    }

    @Test
    public void testRemoveController() throws Exception {
        final var nodes = new TestKitNodes.Builder().
            setNumBrokerNodes(1).
            setNumControllerNodes(3).
            build();

        final Map<Integer, Uuid> initialVoters = new HashMap<>();
        for (final var controllerNode : nodes.controllerNodes().values()) {
            initialVoters.put(
                controllerNode.id(),
                controllerNode.metadataDirectoryId()
            );
        }

        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes).
            setInitialVoterSet(initialVoters).
            build()
        ) {
            cluster.format();
            cluster.startup();
            try (var admin = cluster.admin()) {
                retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(Set.of(3000, 3001, 3002), voters.keySet());
                    for (int replicaId : new int[] {3000, 3001, 3002}) {
                        assertNotEquals(Uuid.ZERO_UUID, voters.get(replicaId));
                    }
                });
                admin.removeRaftVoter(3000, cluster.nodes().
                    controllerNodes().get(3000).metadataDirectoryId()).all().get();
            }
        }
    }

    @Test
    public void testRemoveAndAddSameController() throws Exception {
        final var nodes = new TestKitNodes.Builder().
            setNumBrokerNodes(1).
            setNumControllerNodes(4).
            build();

        final Map<Integer, Uuid> initialVoters = new HashMap<>();
        for (final var controllerNode : nodes.controllerNodes().values()) {
            initialVoters.put(
                controllerNode.id(),
                controllerNode.metadataDirectoryId()
            );
        }

        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes).
            setInitialVoterSet(initialVoters).
            build()
        ) {
            cluster.format();
            cluster.startup();
            try (var admin = cluster.admin()) {
                retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(Set.of(3000, 3001, 3002, 3003), voters.keySet());
                    for (int replicaId : new int[] {3000, 3001, 3002, 3003}) {
                        assertNotEquals(Uuid.ZERO_UUID, voters.get(replicaId));
                    }
                });
                Uuid dirId = cluster.nodes().controllerNodes().get(3000).metadataDirectoryId();
                int port = port(admin, 3000);
                admin.removeRaftVoter(3000, dirId).all().get();
                retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(Set.of(3001, 3002, 3003), voters.keySet());
                    for (int replicaId : new int[] {3001, 3002, 3003}) {
                        assertNotEquals(Uuid.ZERO_UUID, voters.get(replicaId));
                    }
                });

                retryOnExceptionWithTimeout(30_000, 1_000, () ->
                    admin.addRaftVoter(3000, dirId, Set.of(new RaftVoterEndpoint("CONTROLLER", "localhost", port))).all().get());
            }
        }
    }

    @Test
    public void testControllersAutoJoinStandaloneVoter() throws Exception {
        final var nodes = new TestKitNodes.Builder().
            setNumBrokerNodes(1).
            setNumControllerNodes(3).
            build();
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes).
            setConfigProp(QuorumConfig.QUORUM_AUTO_JOIN_ENABLE_CONFIG, true).
            setStandalone(true).
            build()
        ) {
            cluster.format();
            cluster.startup();
            try (var admin = cluster.admin()) {
                retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(Set.of(3000, 3001, 3002), voters.keySet());
                    for (int replicaId : new int[] {3000, 3001, 3002}) {
                        assertEquals(nodes.controllerNodes().get(replicaId).metadataDirectoryId(), voters.get(replicaId));
                    }
                });
            }
        }
    }

    @Test
    public void testNewVoterAutoRemovesAndAdds() throws Exception {
        final var nodes = new TestKitNodes.Builder().
            setNumBrokerNodes(1).
            setNumControllerNodes(3).
            build();

        // Configure the initial voters with one voter having a different directory ID.
        // This simulates the case where the controller failed and is brought back up with a different directory ID.
        final Map<Integer, Uuid> initialVoters = new HashMap<>();
        final var oldDirectoryId = Uuid.randomUuid();
        for (final var controllerNode : nodes.controllerNodes().values()) {
            initialVoters.put(
                controllerNode.id(),
                controllerNode.id() == TestKitDefaults.CONTROLLER_ID_OFFSET ?
                    oldDirectoryId : controllerNode.metadataDirectoryId()
            );
        }

        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes).
            setConfigProp(QuorumConfig.QUORUM_AUTO_JOIN_ENABLE_CONFIG, true).
            setInitialVoterSet(initialVoters).
            build()
        ) {
            cluster.format();
            cluster.startup();
            try (var admin = cluster.admin()) {
                retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(Set.of(3000, 3001, 3002), voters.keySet());
                    for (int replicaId : new int[] {3000, 3001, 3002}) {
                        assertEquals(nodes.controllerNodes().get(replicaId).metadataDirectoryId(), voters.get(replicaId));
                    }
                });
            }
        }
    }

    @Test
    public void testRemoveAndAddVoterWithValidClusterId() throws Exception {
        final var nodes = new TestKitNodes.Builder()
            .setClusterId("test-cluster")
            .setNumBrokerNodes(1)
            .setNumControllerNodes(3)
            .build();

        final Map<Integer, Uuid> initialVoters = new HashMap<>();
        for (final var controllerNode : nodes.controllerNodes().values()) {
            initialVoters.put(
                controllerNode.id(),
                controllerNode.metadataDirectoryId()
            );
        }

        try (var cluster = new KafkaClusterTestKit.Builder(nodes).setInitialVoterSet(initialVoters).build()) {
            cluster.format();
            cluster.startup();
            try (var admin = cluster.admin()) {
                retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(Set.of(3000, 3001, 3002), voters.keySet());
                    for (int replicaId : new int[] {3000, 3001, 3002}) {
                        assertNotEquals(Uuid.ZERO_UUID, voters.get(replicaId));
                    }
                });

                Uuid dirId = cluster.nodes().controllerNodes().get(3000).metadataDirectoryId();
                int port = port(admin, 3000);
                admin.removeRaftVoter(
                    3000,
                    dirId,
                    new RemoveRaftVoterOptions().setClusterId(Optional.of("test-cluster"))
                ).all().get();
                retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(Set.of(3001, 3002), voters.keySet());
                    for (int replicaId : new int[] {3001, 3002}) {
                        assertNotEquals(Uuid.ZERO_UUID, voters.get(replicaId));
                    }
                });

                retryOnExceptionWithTimeout(30_000, 1_000, () ->
                    admin.addRaftVoter(3000, dirId, Set.of(new RaftVoterEndpoint("CONTROLLER", "localhost", port)),
                        new AddRaftVoterOptions().setClusterId(Optional.of("test-cluster"))).all().get());
            }
        }
    }

    @Test
    public void testRemoveAndAddVoterWithInconsistentClusterId() throws Exception {
        final var nodes = new TestKitNodes.Builder()
            .setClusterId("test-cluster")
            .setNumBrokerNodes(1)
            .setNumControllerNodes(3)
            .build();

        final Map<Integer, Uuid> initialVoters = new HashMap<>();
        for (final var controllerNode : nodes.controllerNodes().values()) {
            initialVoters.put(
                controllerNode.id(),
                controllerNode.metadataDirectoryId()
            );
        }

        try (var cluster = new KafkaClusterTestKit.Builder(nodes).setInitialVoterSet(initialVoters).build()) {
            cluster.format();
            cluster.startup();
            try (var admin = cluster.admin()) {
                Uuid dirId = cluster.nodes().controllerNodes().get(3000).metadataDirectoryId();
                var removeFuture = admin.removeRaftVoter(
                    3000,
                    dirId,
                    new RemoveRaftVoterOptions().setClusterId(Optional.of("inconsistent"))
                ).all();
                assertFutureThrows(InconsistentClusterIdException.class, removeFuture);

                var addFuture = admin.addRaftVoter(
                    3000,
                    dirId,
                    Set.of(new RaftVoterEndpoint("CONTROLLER", "localhost", port(admin, 3000))),
                    new AddRaftVoterOptions().setClusterId(Optional.of("inconsistent"))
                ).all();
                assertFutureThrows(InconsistentClusterIdException.class, addFuture);
            }
        }
    }

    @Test
    public void testAddNewControllerFormattedWithFullVoterSet() throws Exception {
        // The test tries to validate that even in case of scale up, the new controller can be formatted with -I with all voters
        // 1. Initial cluster: format 3 controllers with -I "0,1,2"
        // 2. Start those 3 controllers so active quorum [0,1,2]
        // 3. Scale up: format a new 4th controller with -I "0,1,2,3" (includes all)
        // 4. Start the 4th controller so it's an observer (local voter set != active quorum)
        // 5. Run add-controller so now active quorum has [0,1,2,3]

        final var nodes = new TestKitNodes.Builder()
                .setNumBrokerNodes(3)
                // even if the cluster has 3 controllers at the beginning, setting 4 because TestKitNodes is immutable
                // and when building the cluster instance it leverages the controller nodes configured here
                .setNumControllerNodes(4)
                .build();

        // Initial voter set, only first 3 controllers
        final Map<Integer, Uuid> initialThreeVoters = new HashMap<>();
        for (int id : new int[]{3000, 3001, 3002}) {
            initialThreeVoters.put(id, nodes.controllerNodes().get(id).metadataDirectoryId());
        }

        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes)
                .setInitialVoterSet(initialThreeVoters)
                .build()
        ) {
            // manually format only first 3 controllers + all brokers
            // This leaves controller 3003 unformatted for later scale-up simulation

            // Format first 3 controllers with initial voter set
            for (int id : new int[]{3000, 3001, 3002}) {
                cluster.formatController(id, initialThreeVoters);
            }

            // Format all brokers
            for (int brokerId : cluster.brokers().keySet()) {
                cluster.formatBroker(brokerId);
            }

            // Start only the formatted nodes (3 controllers + all brokers)
            for (int id : new int[]{3000, 3001, 3002}) {
                cluster.controllers().get(id).startup();
            }
            for (var broker : cluster.brokers().values()) {
                broker.startup();
            }

            try (var admin = cluster.admin()) {
                // Verify initial 3-controller quorum
                retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(Set.of(3000, 3001, 3002), voters.keySet(),
                            "Initial quorum should only have 3 controllers");
                    for (int replicaId : new int[]{3000, 3001, 3002}) {
                        assertEquals(
                                nodes.controllerNodes().get(replicaId).metadataDirectoryId(),
                                voters.get(replicaId),
                                "Directory ID should match for controller " + replicaId
                        );
                    }
                });

                // Ensure quorum has a stable leader before proceeding
                retryOnExceptionWithTimeout(30_000, 10, () -> {
                    QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
                    assertTrue(quorumInfo.leaderId() >= 3000 && quorumInfo.leaderId() <= 3002,
                            "Quorum should have a stable leader from initial 3 controllers");
                });

                // Scale-up a new controller, format 4th controller with all 4 in voter set
                // This simulates formatting a new controller with -I containing all controllers
                final Map<Integer, Uuid> allFourVoters = new HashMap<>(initialThreeVoters);
                allFourVoters.put(3003, nodes.controllerNodes().get(3003).metadataDirectoryId());

                cluster.formatController(3003, allFourVoters);

                // Start the 4th controller
                cluster.controllers().get(3003).startup();

                // Verify the quorum is stable with 3 voters and 4 observers (3 brokers + controller 3003)
                // This also implicitly verifies controller 3003 is running and connected
                retryOnExceptionWithTimeout(60_000, 10, () -> {
                    QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
                    assertEquals(3, quorumInfo.voters().size(), "Should have 3 voters");
                    assertEquals(4, quorumInfo.observers().size(), "Should have 4 observers (3 brokers + controller 3003)");
                    assertTrue(quorumInfo.leaderId() >= 3000 && quorumInfo.leaderId() <= 3002,
                            "Leader should be one of the 3 initial controllers");
                    assertTrue(quorumInfo.observers().stream()
                            .anyMatch(o -> o.replicaId() == 3003), "Controller 3003 should be an observer");
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    // IMPORTANT!!! formatting with -I all doesn't auto-add to quorum
                    assertEquals(Set.of(3000, 3001, 3002), voters.keySet(),
                            "Controller 3003 should NOT be in active quorum yet despite being formatted with all 4 in voter set");
                });

                int port = cluster.controllers().get(3003).socketServer().boundPort(new ListenerName("CONTROLLER"));
                // Add 4th controller to active quorum via add-controller command
                admin.addRaftVoter(
                    3003,
                    nodes.controllerNodes().get(3003).metadataDirectoryId(),
                    Set.of(new RaftVoterEndpoint("CONTROLLER", "localhost", port))
                ).all().get();

                // Verify all 4 controllers are now in the active quorum
                retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(Set.of(3000, 3001, 3002, 3003), voters.keySet(),
                            "All 4 controllers should be in active quorum after add-controller");
                    for (int replicaId : new int[]{3000, 3001, 3002, 3003}) {
                        assertEquals(
                                nodes.controllerNodes().get(replicaId).metadataDirectoryId(),
                                voters.get(replicaId),
                                "Directory ID should match for controller " + replicaId
                        );
                    }
                });

                // Validate quorum health
                QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
                assertEquals(4, quorumInfo.voters().size(), "Should have 4 voters");
                assertTrue(quorumInfo.leaderId() >= 3000 && quorumInfo.leaderId() <= 3003,
                        "Leader should be one of the 4 controllers");
            }
        }
    }

    private static int port(Admin admin, int nodeId) throws Exception {
        return admin.describeMetadataQuorum().quorumInfo().get().nodes().get(nodeId).endpoints().stream()
            .findFirst().orElseThrow().port();
    }
}