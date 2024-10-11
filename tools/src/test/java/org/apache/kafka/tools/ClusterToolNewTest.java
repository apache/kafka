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
package org.apache.kafka.tools;

import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;


import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(clusterType = Type.ALL, brokers = 3, serverProperties = {
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
        @ClusterConfigProperty(key = "auto.leader.rebalance.enable", value = "false"),
        @ClusterConfigProperty(key = "controlled.shutdown.enable", value = "true"),
        @ClusterConfigProperty(key = "controlled.shutdown.max.retries", value = "1"),
        @ClusterConfigProperty(key = "controlled.shutdown.retry.backoff.ms", value = "1000"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "2")
})
@Tag("integration")
public class ClusterToolNewTest {
    private final ClusterInstance cluster;

    public ClusterToolNewTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testPrintClusterId() throws Exception {
        try (Admin adminClient = cluster.createAdminClient()) {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            ClusterTool.clusterIdCommand(new PrintStream(stream), adminClient);
            assertEquals(String.format("Cluster ID: %s\n", cluster.clusterId()), stream.toString());
        }
    }

    @ClusterTest(clusterType = Type.KRAFT, metadataVersion = MetadataVersion.IBP_1_0_IV0)
    public void testClusterTooOldToHaveId() throws Exception {
        try (Admin adminClient = cluster.createAdminClient()) {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            ClusterTool.clusterIdCommand(new PrintStream(stream), adminClient);
            assertEquals("No cluster ID found. The Kafka version is probably too old.\n", stream.toString());
        }
    }

    @ClusterTest(clusterType = Type.KRAFT)
    public void testUnregisterBroker() throws Exception {
        try (Admin adminClient = cluster.createAdminClient()) {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            ClusterTool.unregisterCommand(new PrintStream(stream), adminClient, 0);
            assertEquals("Broker 0 is no longer registered.\n", stream.toString());
        }
    }

    @ClusterTest(clusterType = Type.ZK)
    public void testLegacyModeClusterCannotUnregisterBroker() throws Exception {
        try (Admin adminClient = cluster.createAdminClient()) {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            ClusterTool.unregisterCommand(new PrintStream(stream), adminClient, 0);
            assertEquals("The target cluster does not support the broker unregistration API.\n", stream.toString());
        }
    }

}
