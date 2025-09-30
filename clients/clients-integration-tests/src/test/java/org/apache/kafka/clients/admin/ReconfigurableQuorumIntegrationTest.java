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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InconsistentClusterIdException;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class ReconfigurableQuorumIntegrationTest {

    static Map<Integer, Uuid> descVoterDirs(Admin admin) throws ExecutionException, InterruptedException {
        var quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
        return quorumInfo.voters().stream().collect(Collectors.toMap(QuorumInfo.ReplicaState::replicaId, QuorumInfo.ReplicaState::replicaDirectoryId));
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
            try (Admin admin = Admin.create(cluster.clientProperties())) {
                TestUtils.waitForCondition(() -> {
                    Map<Integer, Uuid> voters = descVoterDirs(admin);
                    assertEquals(Set.of(3000, 3001, 3002), voters.keySet());
                    return voters.values().stream().noneMatch(directory -> directory.equals(Uuid.ZERO_UUID));
                }, "Initial quorum voters should be {3000, 3001, 3002} and all should have non-zero directory IDs");

                Uuid dirId = cluster.nodes().controllerNodes().get(3000).metadataDirectoryId();
                admin.removeRaftVoter(
                        3000,
                        dirId,
                        new RemoveRaftVoterOptions().setClusterId(Optional.of("test-cluster"))
                ).all().get();
                TestUtils.waitForCondition(() -> {
                    Map<Integer, Uuid> voters = descVoterDirs(admin);
                    assertEquals(Set.of(3001, 3002), voters.keySet());
                    return voters.values().stream().noneMatch(directory -> directory.equals(Uuid.ZERO_UUID));
                }, "After removing voter 3000, remaining voters should be {3001, 3002} with non-zero directory IDs");

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
            try (Admin admin = Admin.create(cluster.clientProperties())) {
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
