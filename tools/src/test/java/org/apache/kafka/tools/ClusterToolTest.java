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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusterToolTest {

    @ClusterTest
    public void testClusterId(ClusterInstance clusterInstance) {
        String output = ToolsTestUtils.captureStandardOut(() ->
                assertDoesNotThrow(() -> ClusterTool.execute("cluster-id", "--bootstrap-server", clusterInstance.bootstrapServers())));
        assertTrue(output.contains("Cluster ID: " + clusterInstance.clusterId()));
    }

    @ClusterTest(brokers = 3)
    public void testUnregister(ClusterInstance clusterInstance) {
        int brokerId;
        Set<Integer> brokerIds = clusterInstance.brokerIds();
        brokerIds.removeAll(clusterInstance.controllerIds());
        brokerId = assertDoesNotThrow(() -> brokerIds.stream().findFirst().get());
        clusterInstance.shutdownBroker(brokerId);
        String output = ToolsTestUtils.captureStandardOut(() ->
                assertDoesNotThrow(() -> ClusterTool.execute("unregister", "--bootstrap-server", clusterInstance.bootstrapServers(), "--id", String.valueOf(brokerId))));

        assertTrue(output.contains("Broker " + brokerId + " is no longer registered."));
    }

    @ClusterTest(brokers = 1, types = {Type.KRAFT, Type.CO_KRAFT})
    public void testListEndpointsWithBootstrapServer(ClusterInstance clusterInstance) {
        String output = ToolsTestUtils.captureStandardOut(() ->
                assertDoesNotThrow(() -> ClusterTool.execute("list-endpoints", "--bootstrap-server", clusterInstance.bootstrapServers())));
        String port = clusterInstance.bootstrapServers().split(":")[1];
        int id = clusterInstance.brokerIds().iterator().next();
        String format = "%-10s %-9s %-10s %-10s %-10s %-15s%n%-10s %-9s %-10s %-10s %-10s %-6s";
        String expected = String.format(format, "ID", "HOST", "PORT", "RACK", "STATE", "ENDPOINT_TYPE", id, "localhost", port, "null", "unfenced", "broker");
        assertEquals(expected, output);
    }

    @ClusterTest(brokers = 2, types = {Type.KRAFT, Type.CO_KRAFT})
    public void testListEndpointsArgumentWithBootstrapServer(ClusterInstance clusterInstance) {
        List<Integer> brokerIds = clusterInstance.brokerIds().stream().toList();
        clusterInstance.shutdownBroker(brokerIds.get(0));

        List<String> ports = Arrays.stream(clusterInstance.bootstrapServers().split(",")).map(b ->  b.split(":")[1]).toList();
        String format = "%-10s %-9s %-10s %-10s %-10s %-15s%n%-10s %-9s %-10s %-10s %-10s %-15s%n%-10s %-9s %-10s %-10s %-10s %-6s";
        String expected = String.format(format,
                "ID", "HOST", "PORT", "RACK", "STATE", "ENDPOINT_TYPE",
                brokerIds.get(0), "localhost", ports.get(0), "null", "fenced", "broker",
                brokerIds.get(1), "localhost", ports.get(1), "null", "unfenced", "broker");

        String output = ToolsTestUtils.captureStandardOut(() -> assertDoesNotThrow(() -> ClusterTool.execute("list-endpoints", "--bootstrap-server", clusterInstance.bootstrapServers(), "--include-fenced-brokers")));

        assertEquals(expected, output);
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT})
    public void testClusterIdWithBootstrapController(ClusterInstance clusterInstance) {
        String output = ToolsTestUtils.captureStandardOut(() ->
                assertDoesNotThrow(() -> ClusterTool.execute("cluster-id", "--bootstrap-controller", clusterInstance.bootstrapControllers())));
        assertTrue(output.contains("Cluster ID: " + clusterInstance.clusterId()));
    }

    @ClusterTest(brokers = 3, types = {Type.KRAFT, Type.CO_KRAFT})
    public void testUnregisterWithBootstrapController(ClusterInstance clusterInstance) {
        Set<Integer> brokerIds = clusterInstance.brokerIds();
        brokerIds.removeAll(clusterInstance.controllerIds());
        int brokerId = assertDoesNotThrow(() -> brokerIds.stream().findFirst().get());
        clusterInstance.shutdownBroker(brokerId);
        assertDoesNotThrow(() -> ClusterTool.execute("unregister", "--bootstrap-controller", clusterInstance.bootstrapControllers(), "--id", String.valueOf(brokerId)));
    }

    @ClusterTest(brokers = 3, types = {Type.KRAFT, Type.CO_KRAFT})
    public void testListEndpointsWithBootstrapController(ClusterInstance clusterInstance) {
        String output = ToolsTestUtils.captureStandardOut(() ->
                assertDoesNotThrow(() -> ClusterTool.execute("list-endpoints", "--bootstrap-controller", clusterInstance.bootstrapControllers())));
        String port = clusterInstance.bootstrapControllers().split(":")[1];
        int id = clusterInstance.controllerIds().iterator().next();
        String format = "%-10s %-9s %-10s %-10s %-15s%n%-10s %-9s %-10s %-10s %-10s";
        String expected = String.format(format, "ID", "HOST", "PORT", "RACK", "ENDPOINT_TYPE", id, "localhost", port, "null", "controller");
        assertEquals(expected, output);
    }

    @ClusterTest(brokers = 3, types = {Type.KRAFT, Type.CO_KRAFT})
    public void testListEndpointsArgumentWithBootstrapController(ClusterInstance clusterInstance) {
        RuntimeException exception =
                assertThrows(RuntimeException.class,
                        () -> ClusterTool.execute("list-endpoints", "--bootstrap-controller", clusterInstance.bootstrapControllers(), "--include-fenced-brokers"));
        assertEquals("The option --include-fenced-brokers is only supported with --bootstrap-server option", exception.getMessage());
    }

    @ClusterTest
    public void testDeprecatedConfig(ClusterInstance clusterInstance) throws IOException {
        File configFile = TestUtils.tempFile("client.id=my-client");

        try (final MockedStatic<Admin> mockedAdmin = Mockito.mockStatic(Admin.class, Mockito.CALLS_REAL_METHODS)) {
            String output = ToolsTestUtils.captureStandardOut(() ->
                assertDoesNotThrow(() -> ClusterTool.execute(
                    "cluster-id",
                    "--bootstrap-server", clusterInstance.bootstrapServers(),
                    "--config", configFile.getAbsolutePath()
                ))
            );
            assertTrue(output.contains("Option --config has been deprecated and will be removed in a future version. Use --command-config instead."));

            ArgumentCaptor<Properties> argumentCaptor = ArgumentCaptor.forClass(Properties.class);
            mockedAdmin.verify(() -> Admin.create(argumentCaptor.capture()));
            final Properties actualProps = argumentCaptor.getValue();
            assertEquals("my-client", actualProps.get(AdminClientConfig.CLIENT_ID_CONFIG));
        }
    }

    @ClusterTest
    public void testCommandConfig(ClusterInstance clusterInstance) throws IOException {
        File configFile = TestUtils.tempFile("client.id=my-client");

        try (final MockedStatic<Admin> mockedAdmin = Mockito.mockStatic(Admin.class, Mockito.CALLS_REAL_METHODS)) {
            String output = ToolsTestUtils.captureStandardOut(() ->
                assertDoesNotThrow(() -> ClusterTool.execute(
                    "cluster-id",
                    "--bootstrap-server", clusterInstance.bootstrapServers(),
                    "--command-config", configFile.getAbsolutePath()
                ))
            );
            assertTrue(output.contains("Cluster ID: " + clusterInstance.clusterId()));

            ArgumentCaptor<Properties> argumentCaptor = ArgumentCaptor.forClass(Properties.class);
            mockedAdmin.verify(() -> Admin.create(argumentCaptor.capture()));
            final Properties actualProps = argumentCaptor.getValue();
            assertEquals("my-client", actualProps.get(AdminClientConfig.CLIENT_ID_CONFIG));
        }
    }

    @ClusterTest
    public void testCommandConfigAndDeprecatedConfigPresent(ClusterInstance clusterInstance) throws IOException {
        File configFile = TestUtils.tempFile("client.id=my-client");

        ArgumentParserException ex = assertThrows(ArgumentParserException.class, () ->
            ClusterTool.execute(
                "cluster-id",
                "--bootstrap-server", clusterInstance.bootstrapServers(),
                "--config", configFile.getAbsolutePath(),
                "--command-config", configFile.getAbsolutePath()
            )
        );
        assertEquals("--config and --command-config cannot be specified together.", ex.getMessage());
    }

    @Test
    public void testPrintClusterId() throws Exception {
        Admin adminClient = new MockAdminClient.Builder().
                clusterId("QtNwvtfVQ3GEFpzOmDEE-w").
                build();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ClusterTool.clusterIdCommand(new PrintStream(stream), adminClient);
        assertEquals("Cluster ID: QtNwvtfVQ3GEFpzOmDEE-w\n", stream.toString());
    }

    @Test
    public void testClusterTooOldToHaveId() throws Exception {
        Admin adminClient = new MockAdminClient.Builder().
                clusterId(null).
                build();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ClusterTool.clusterIdCommand(new PrintStream(stream), adminClient);
        assertEquals("No cluster ID found. The Kafka version is probably too old.\n", stream.toString());
    }

    @Test
    public void testUnregisterBroker() throws Exception {
        Admin adminClient = new MockAdminClient.Builder().numBrokers(3).
                usingRaftController(true).
                build();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ClusterTool.unregisterCommand(new PrintStream(stream), adminClient, 0);
        assertEquals("Broker 0 is no longer registered.\n", stream.toString());
    }

    @Test
    public void testLegacyModeClusterCannotUnregisterBroker() throws Exception {
        Admin adminClient = new MockAdminClient.Builder().numBrokers(3).
                usingRaftController(false).
                build();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ClusterTool.unregisterCommand(new PrintStream(stream), adminClient, 0);
        assertEquals("The target cluster does not support the broker unregistration API.\n", stream.toString());
    }
}
