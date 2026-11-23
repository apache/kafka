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

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.test.TestUtils;

import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

public class DynamicBrokerConfigTest {

    @ClusterTest(types = {Type.KRAFT})
    public void testIncreaseNumIoThreads(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            admin.incrementalAlterConfigs(Map.of(
                new ConfigResource(ConfigResource.Type.BROKER, ""),
                List.of(new AlterConfigOp(
                    new ConfigEntry(ServerConfigs.NUM_IO_THREADS_CONFIG, Integer.toString(ServerConfigs.NUM_IO_THREADS_DEFAULT + 1)),
                    AlterConfigOp.OpType.SET))
            )).all().get();

            // KAFKA-16649 introduced this coverage as a safeguard against future deadlocks.
            // Verify that the broker continues serving requests after resizing the I/O thread pool.
            cluster.createTopic("test-topic", 1, (short) 1);
        }
    }

    @ClusterTest(types = {Type.KRAFT},
        brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        serverProperties = @ClusterConfigProperty(key = SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG, value = "4"))
    public void testChannelBuildersRemovedOnNetworkThreadShrink(ClusterInstance cluster) throws Exception {
        var brokerConfig = cluster.brokers().get(0).config();
        LongSupplier channelBuilderCount = () -> brokerConfig.dynamicConfig().reconfigurables().stream()
            .filter(r -> r instanceof ChannelBuilder).count();
        // Other broker components also register builders, so expect one fewer SASL builder per removed processor.
        long expectedAfterShrink = channelBuilderCount.getAsLong() - 2;

        try (Admin admin = cluster.admin()) {
            admin.incrementalAlterConfigs(Map.of(
                new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerConfig.brokerId())),
                List.of(new AlterConfigOp(new ConfigEntry(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG, "2"), AlterConfigOp.OpType.SET))
            )).all().get();
        }
        TestUtils.waitForCondition(() -> channelBuilderCount.getAsLong() == expectedAfterShrink,
            () -> "Expected " + expectedAfterShrink + " registered channel builders after shrink, found " + channelBuilderCount.getAsLong());
    }
}
