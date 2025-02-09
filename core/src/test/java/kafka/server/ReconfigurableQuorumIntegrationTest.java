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

package kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.RaftVoterEndpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReconfigurableQuorumIntegrationTest {
    static void checkKRaftVersions(Admin admin, short finalized) throws Exception {
        FeatureMetadata featureMetadata = admin.describeFeatures().featureMetadata().get();
        if (finalized > 0) {
            assertTrue(featureMetadata.finalizedFeatures().containsKey(KRaftVersion.FEATURE_NAME));
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
                    build()).build()
        ) {
            cluster.format();
            cluster.startup();
            try (Admin admin = Admin.create(cluster.clientProperties())) {
                TestUtils.retryOnExceptionWithTimeout(30_000, () -> {
                    checkKRaftVersions(admin, (short) 0);
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
                setFeature(KRaftVersion.FEATURE_NAME, (short) 1).
                    build()).build()
        ) {
            cluster.format();
            cluster.startup();
            try (Admin admin = Admin.create(cluster.clientProperties())) {
                TestUtils.retryOnExceptionWithTimeout(30_000, () -> {
                    checkKRaftVersions(admin, (short) 1);
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
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder().
                setNumBrokerNodes(1).
                setNumControllerNodes(3).
                setFeature(KRaftVersion.FEATURE_NAME, (short) 1).
                    build()).build()
        ) {
            cluster.format();
            cluster.startup();
            try (Admin admin = Admin.create(cluster.clientProperties())) {
                TestUtils.retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(new HashSet<>(Arrays.asList(3000, 3001, 3002)), voters.keySet());
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
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder().
                setNumBrokerNodes(1).
                setNumControllerNodes(4).
                setFeature(KRaftVersion.FEATURE_NAME, (short) 1).
                build()).build()
        ) {
            cluster.format();
            cluster.startup();
            try (Admin admin = Admin.create(cluster.clientProperties())) {
                TestUtils.retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(new HashSet<>(Arrays.asList(3000, 3001, 3002, 3003)), voters.keySet());
                    for (int replicaId : new int[] {3000, 3001, 3002, 3003}) {
                        assertNotEquals(Uuid.ZERO_UUID, voters.get(replicaId));
                    }
                });
                Uuid dirId = cluster.nodes().controllerNodes().get(3000).metadataDirectoryId();
                admin.removeRaftVoter(3000, dirId).all().get();
                TestUtils.retryOnExceptionWithTimeout(30_000, 10, () -> {
                    Map<Integer, Uuid> voters = findVoterDirs(admin);
                    assertEquals(new HashSet<>(Arrays.asList(3001, 3002, 3003)), voters.keySet());
                    for (int replicaId : new int[] {3001, 3002, 3003}) {
                        assertNotEquals(Uuid.ZERO_UUID, voters.get(replicaId));
                    }
                });
                admin.addRaftVoter(
                    3000,
                    dirId,
                    Collections.singleton(new RaftVoterEndpoint("CONTROLLER", "example.com", 8080))
                ).all().get();
            }
        }
    }
}
