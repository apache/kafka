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
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.test.TestUtils;

import java.util.List;
import java.util.Map;

public class DynamicBrokerConfigIntegrationTest {

    @ClusterTest(
        types = {Type.KRAFT},
        brokers = 1,
        controllers = 1,
        serverProperties = {
            @ClusterConfigProperty(key = ServerConfigs.NUM_IO_THREADS_CONFIG, value = "4")
        }
    )
    public void testIncreaseNumIoThreads(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            admin.incrementalAlterConfigs(Map.of(
                new ConfigResource(ConfigResource.Type.BROKER, ""),
                List.of(new AlterConfigOp(new ConfigEntry(ServerConfigs.NUM_IO_THREADS_CONFIG, "8"), AlterConfigOp.OpType.SET)))
            ).all().get();

            admin.createTopics(List.of(new NewTopic("test-topic", 1, (short) 1))).all().get();
            TestUtils.waitForCondition(
                () -> admin.listTopics().names().get().contains("test-topic"),
                "Failed to find test-topic"
            );
        }
    }
}
