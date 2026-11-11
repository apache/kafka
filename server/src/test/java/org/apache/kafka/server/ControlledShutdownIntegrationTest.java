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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.test.TestUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG, value = "15000")
    }
)
public class ControlledShutdownIntegrationTest {
    private static final String TOPIC = "test-topic";
    private static final int SHUTDOWN_BROKER_ID = 0;

    private final ClusterInstance clusterInstance;

    ControlledShutdownIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "true")
    })
    public void testLeadershipMovedBeforeShutdownCompletes() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            createTopicLedByShutdownBroker(admin);

            clusterInstance.shutdownBroker(SHUTDOWN_BROKER_ID);
            clusterInstance.ensureConsistentMetadata();

            TopicPartitionInfo partition = describePartition(admin);
            assertTrue(leaderMovedOff(partition), partition.toString());
        }
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "false")
    })
    public void testLeadershipMovedOnlyAfterSessionTimeoutWhenControlledShutdownDisabled() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            createTopicLedByShutdownBroker(admin);

            clusterInstance.shutdownBroker(SHUTDOWN_BROKER_ID);
            clusterInstance.ensureConsistentMetadata();

            TopicPartitionInfo partition = describePartition(admin);
            assertNotNull(partition.leader(), partition.toString());
            assertEquals(SHUTDOWN_BROKER_ID, partition.leader().id(), partition.toString());
            assertEquals(List.of(0, 1, 2), isrIds(partition), partition.toString());

            TestUtils.waitForCondition(() -> leaderMovedOff(describePartition(admin)),
                60_000L, "Leadership was not moved after the broker session timed out");
        }
    }

    private void createTopicLedByShutdownBroker(Admin admin) throws Exception {
        clusterInstance.createTopicWithAssignment(TOPIC, Map.of(0, List.of(SHUTDOWN_BROKER_ID, 1, 2)));
        TestUtils.waitForCondition(() -> {
            TopicPartitionInfo info = describePartition(admin);
            return info.leader() != null && info.leader().id() == SHUTDOWN_BROKER_ID && isrIds(info).size() == 3;
        }, "Partition was not led by broker " + SHUTDOWN_BROKER_ID + " with a full ISR");
    }

    private static TopicPartitionInfo describePartition(Admin admin) throws ExecutionException, InterruptedException {
        return admin.describeTopics(List.of(TOPIC)).allTopicNames().get().get(TOPIC).partitions().get(0);
    }

    private static boolean leaderMovedOff(TopicPartitionInfo info) {
        return info.leader() != null && info.leader().id() != SHUTDOWN_BROKER_ID &&
            isrIds(info).stream().noneMatch(id -> id == SHUTDOWN_BROKER_ID);
    }

    private static List<Integer> isrIds(TopicPartitionInfo partition) {
        return partition.isr().stream().map(Node::id).sorted().toList();
    }
}
