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
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.test.api.TestKitDefaults;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class ReconfigurableQuorumIntegrationTest {
    static void checkKRaftVersions(Admin admin, short finalized) throws Exception {
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
                TestUtils.retryOnExceptionWithTimeout(30_000, () -> {
                    checkKRaftVersions(admin, KRaftVersion.KRAFT_VERSION_0.featureLevel());
                });
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
                TestUtils.retryOnExceptionWithTimeout(30_000, () -> {
                    checkKRaftVersions(admin, KRaftVersion.KRAFT_VERSION_1.featureLevel());
                });
            }
        }
    }

    static Map<Integer, Uuid> findVoterDirs(Admin admin) throws Exception {
        QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
        Map<Integer, Uuid> result = new TreeMap<>();
        quorumInfo.voters().forEach(v -> {
            result.put(v.replicaId(), v.replicaDirectoryId());
        });
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
                TestUtils.retryOnExceptionWithTimeout(30_000, 10, () -> {
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
                TestUtils.retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(Set.of(3000, 3001, 3002, 3003), voters.keySet());
                    for (int replicaId : new int[] {3000, 3001, 3002, 3003}) {
                        assertNotEquals(Uuid.ZERO_UUID, voters.get(replicaId));
                    }
                });
                Uuid dirId = cluster.nodes().controllerNodes().get(3000).metadataDirectoryId();
                admin.removeRaftVoter(3000, dirId).all().get();
                TestUtils.retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(Set.of(3001, 3002, 3003), voters.keySet());
                    for (int replicaId : new int[] {3001, 3002, 3003}) {
                        assertNotEquals(Uuid.ZERO_UUID, voters.get(replicaId));
                    }
                });
                admin.addRaftVoter(
                    3000,
                    dirId,
                    Set.of(new RaftVoterEndpoint("CONTROLLER", "example.com", 8080))
                ).all().get();
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
                TestUtils.retryOnExceptionWithTimeout(30_000, 10, () -> {
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
                TestUtils.retryOnExceptionWithTimeout(30_000, 10, () -> {
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
                TestUtils.retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(Set.of(3000, 3001, 3002), voters.keySet());
                    for (int replicaId : new int[] {3000, 3001, 3002}) {
                        assertNotEquals(Uuid.ZERO_UUID, voters.get(replicaId));
                    }
                });

                Uuid dirId = cluster.nodes().controllerNodes().get(3000).metadataDirectoryId();
                admin.removeRaftVoter(
                    3000,
                    dirId,
                    new RemoveRaftVoterOptions().setClusterId(Optional.of("test-cluster"))
                ).all().get();
                TestUtils.retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(Set.of(3001, 3002), voters.keySet());
                    for (int replicaId : new int[] {3001, 3002}) {
                        assertNotEquals(Uuid.ZERO_UUID, voters.get(replicaId));
                    }
                });

                admin.addRaftVoter(
                    3000,
                    dirId,
                    Set.of(new RaftVoterEndpoint("CONTROLLER", "example.com", 8080)),
                    new AddRaftVoterOptions().setClusterId(Optional.of("test-cluster"))
                ).all().get();
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
                TestUtils.assertFutureThrows(InconsistentClusterIdException.class, removeFuture);

                var addFuture = admin.addRaftVoter(
                    3000,
                    dirId,
                    Set.of(new RaftVoterEndpoint("CONTROLLER", "example.com", 8080)),
                    new AddRaftVoterOptions().setClusterId(Optional.of("inconsistent"))
                ).all();
                TestUtils.assertFutureThrows(InconsistentClusterIdException.class, addFuture);
            }
        }
    }
}