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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.config.ServerConfigs;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(types = {Type.KRAFT}, serverProperties = {
    @ClusterConfigProperty(key = ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, value = "true"),
})
public class BrokerApiVersionsCommandTest {
    @ClusterTest
    public void testBrokerApiVersionsCommandOutput(ClusterInstance clusterInstance) {
        testBrokerApiVersionsCommandOutput(clusterInstance, false);
        testBrokerApiVersionsCommandOutput(clusterInstance, true);
    }

    private void testBrokerApiVersionsCommandOutput(ClusterInstance clusterInstance, boolean usingBootstrapController) {
        String output = ToolsTestUtils.grabConsoleOutput(() -> {
            if (usingBootstrapController) {
                BrokerApiVersionsCommand.mainNoExit("--bootstrap-controller", clusterInstance.bootstrapControllers());
            } else {
                BrokerApiVersionsCommand.mainNoExit("--bootstrap-server", clusterInstance.bootstrapServers());
            }
        });
        Iterator<String> lineIter = Arrays.stream(output.split("\n")).iterator();
        assertTrue(lineIter.hasNext());
        ApiMessageType.ListenerType listenerType;

        if (usingBootstrapController) {
            assertEquals(clusterInstance.bootstrapControllers() + " (id: 3000 rack: null isFenced: false) -> (", lineIter.next());
            listenerType = ApiMessageType.ListenerType.CONTROLLER;
        } else {
            assertEquals(clusterInstance.bootstrapServers() + " (id: 0 rack: null isFenced: false) -> (", lineIter.next());
            listenerType = ApiMessageType.ListenerType.BROKER;
        }

        EnumSet<ApiKeys> apiKeys = ApiKeys.clientApis();

        // Controller will return all apis
        if (listenerType == ApiMessageType.ListenerType.CONTROLLER) {
            apiKeys.addAll(ApiKeys.controllerApis());
        }

        Iterator<ApiKeys> apiKeysIter = apiKeys.iterator();

        NodeApiVersions nodeApiVersions = new NodeApiVersions(
                ApiVersionsResponse.filterApis(listenerType, true, true),
                List.of());
        while (apiKeysIter.hasNext()) {
            ApiKeys apiKey = apiKeysIter.next();
            String terminator = apiKeysIter.hasNext() ? "," : "";
            StringBuilder lineBuilder = new StringBuilder().append("\t");
            if (apiKey.inScope(listenerType)) {
                ApiVersion apiVersion = nodeApiVersions.apiVersion(apiKey);
                assertNotNull(apiVersion, "No apiVersion found for " + apiKey);

                String versionRangeStr = (apiVersion.minVersion() == apiVersion.maxVersion()) ?
                        String.valueOf(apiVersion.minVersion()) :
                        apiVersion.minVersion() + " to " + apiVersion.maxVersion();
                short usableVersion = nodeApiVersions.latestUsableVersion(apiKey);
                if (apiKey == ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS || apiKey == ApiKeys.PUSH_TELEMETRY) {
                    lineBuilder.append(apiKey.name).append("(").append(apiKey.id).append("): UNSUPPORTED").append(terminator);
                } else {
                    lineBuilder.append(apiKey.name).append("(").append(apiKey.id).append("): ").append(versionRangeStr).append(" [usable: ").append(usableVersion).append("]").append(terminator);
                }
            } else {
                lineBuilder.append(apiKey.name).append("(").append(apiKey.id).append("): UNSUPPORTED").append(terminator);
            }
            assertTrue(lineIter.hasNext());
            assertEquals(lineBuilder.toString(), lineIter.next());
        }
        assertTrue(lineIter.hasNext());
        assertEquals(")", lineIter.next());
        assertFalse(lineIter.hasNext());
    }

    @ClusterTest
    public void testAdminSendNoBlock(ClusterInstance clusterInstance) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        try (BrokerApiVersionsCommand.AdminClient admin = BrokerApiVersionsCommand.AdminClient.create(props, false)) {
            int brokerId = clusterInstance.brokers().keySet().iterator().next();
            KafkaFuture<NodeApiVersions> future =  admin.getNodeApiVersions(new Node(brokerId + 1, "localhost", 9093, null));
            assertTrue(future.isCompletedExceptionally());
        }
    }
}
