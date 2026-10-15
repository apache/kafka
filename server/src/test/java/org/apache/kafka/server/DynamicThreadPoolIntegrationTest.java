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

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.test.TestUtils;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamicThreadPoolIntegrationTest {

    private static final String STATIC_IO_THREADS = "16";
    private static final String STATIC_NETWORK_THREADS = "8";
    private static final ConfigResource CLUSTER_DEFAULT = new ConfigResource(ConfigResource.Type.BROKER, "");

    @ClusterTest(
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = ServerConfigs.NUM_IO_THREADS_CONFIG, value = STATIC_IO_THREADS),
            @ClusterConfigProperty(key = SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG, value = STATIC_NETWORK_THREADS)
        }
    )
    public void testDynamicThreadCountsAreAppliedAfterRestart(ClusterInstance clusterInstance) throws Exception {
        int brokerId = clusterInstance.brokerIds().iterator().next();
        try (Admin admin = clusterInstance.admin()) {
            // Each step is within the allowed bounds, but the final values are less than half the static ones
            setThreadCounts(clusterInstance, admin, brokerId, 8, 4);
            setThreadCounts(clusterInstance, admin, brokerId, 6, 3);
        }

        // The restarted broker is created from the current config, which includes the dynamic values,
        // so restore the static ones
        clusterInstance.restartBroker(brokerId, Map.of(
            ServerConfigs.NUM_IO_THREADS_CONFIG, STATIC_IO_THREADS,
            SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG, STATIC_NETWORK_THREADS));
        clusterInstance.waitForReadyBrokers();

        KafkaBroker broker = clusterInstance.brokers().get(brokerId);
        assertEquals(6, broker.config().numIoThreads());
        assertEquals(6, broker.dataPlaneRequestHandlerPool().threadPoolSize().get());
        assertEquals(3, broker.config().numNetworkThreads());
    }

    private void setThreadCounts(ClusterInstance clusterInstance, Admin admin, int brokerId, int ioThreads, int networkThreads) throws Exception {
        admin.incrementalAlterConfigs(Map.of(CLUSTER_DEFAULT, List.of(
            new AlterConfigOp(new ConfigEntry(ServerConfigs.NUM_IO_THREADS_CONFIG, String.valueOf(ioThreads)), AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG, String.valueOf(networkThreads)), AlterConfigOp.OpType.SET)
        ))).all().get();
        TestUtils.waitForCondition(() -> {
            KafkaBroker broker = clusterInstance.brokers().get(brokerId);
            return broker.config().numIoThreads() == ioThreads && broker.config().numNetworkThreads() == networkThreads;
        }, "Thread counts were not updated to num.io.threads=" + ioThreads + ", num.network.threads=" + networkThreads);
    }
}
