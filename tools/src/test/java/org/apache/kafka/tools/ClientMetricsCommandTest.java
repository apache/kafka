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

import kafka.utils.Exit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListClientMetricsResourcesResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClientMetricsCommandTest {
    private String bootstrapServer = "localhost:9092";
    private String clientMetricsName = "cm";

    @Test
    public void testOptionsNoActionFails() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer});
    }

    @Test
    public void testOptionsListSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--list"});
        assertTrue(opts.hasListOption());
    }

    @Test
    public void testOptionsDescribeNoNameSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--describe"});
        assertTrue(opts.hasDescribeOption());
    }

    @Test
    public void testOptionsDescribeWithNameSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--describe", "--name", clientMetricsName});
        assertTrue(opts.hasDescribeOption());
    }

    @Test
    public void testOptionsDeleteNoNameFails() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer, "--delete"});
    }

    @Test
    public void testOptionsDeleteWithNameSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--delete", "--name", clientMetricsName});
        assertTrue(opts.hasDeleteOption());
    }

    @Test
    public void testOptionsAlterNoNameFails() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer, "--alter"});
    }

    @Test
    public void testOptionsAlterGenerateNameSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--alter", "--generate-name"});
        assertTrue(opts.hasAlterOption());
    }

    @Test
    public void testOptionsAlterWithNameSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--alter", "--name", clientMetricsName});
        assertTrue(opts.hasAlterOption());
    }

    @Test
    public void testOptionsAlterAllOptionsSucceeds() {
        ClientMetricsCommand.ClientMetricsCommandOptions opts = new ClientMetricsCommand.ClientMetricsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--alter", "--name", clientMetricsName,
                "--interval", "1000", "--match", "client_id=abc", "--metrics", "org.apache.kafka."});
        assertTrue(opts.hasAlterOption());

    }

    @Test
    public void testAlter() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        AlterConfigsResult result = AdminClientTestUtils.alterConfigsResult(new ConfigResource(ConfigResource.Type.CLIENT_METRICS, clientMetricsName));
        when(adminClient.incrementalAlterConfigs(any(), any())).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.alterClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--alter",
                                     "--name", clientMetricsName, "--metrics", "org.apache.kafka.producer.",
                                     "--interval", "5000", "--match", "client_id=CLIENT1"}));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertTrue(capturedOutput.contains("Altered client metrics config for " + clientMetricsName + "."));
    }

    @Test
    public void testAlterGenerateName() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        AlterConfigsResult result = AdminClientTestUtils.alterConfigsResult(new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "whatever"));
        when(adminClient.incrementalAlterConfigs(any(), any())).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.alterClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--alter",
                                     "--generate-name", "--metrics", "org.apache.kafka.producer.",
                                     "--interval", "5000", "--match", "client_id=CLIENT1"}));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertTrue(capturedOutput.contains("Altered client metrics config"));
    }

    @Test
    public void testDelete() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        ConfigResource cr = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, clientMetricsName);
        Config cfg = new Config(Collections.singleton(new ConfigEntry("metrics", "org.apache.kafka.producer.")));
        DescribeConfigsResult describeResult = AdminClientTestUtils.describeConfigsResult(cr, cfg);
        when(adminClient.describeConfigs(any())).thenReturn(describeResult);
        AlterConfigsResult alterResult = AdminClientTestUtils.alterConfigsResult(cr);
        when(adminClient.incrementalAlterConfigs(any(), any())).thenReturn(alterResult);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.deleteClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--delete",
                                     "--name", clientMetricsName}));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertTrue(capturedOutput.contains("Deleted client metrics config for " + clientMetricsName + "."));
    }

    @Test
    public void testDescribe() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        ConfigResource cr = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, clientMetricsName);
        Config cfg = new Config(Collections.singleton(new ConfigEntry("metrics", "org.apache.kafka.producer.")));
        DescribeConfigsResult describeResult = AdminClientTestUtils.describeConfigsResult(cr, cfg);
        when(adminClient.describeConfigs(any())).thenReturn(describeResult);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.describeClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--describe",
                                     "--name", clientMetricsName}));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertTrue(capturedOutput.contains("Client metrics configs for " + clientMetricsName + " are:"));
        assertTrue(capturedOutput.contains("metrics=org.apache.kafka.producer."));
    }

    @Test
    public void testDescribeAll() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        ListClientMetricsResourcesResult result = AdminClientTestUtils.listClientMetricsResourcesResult(clientMetricsName);
        when(adminClient.listClientMetricsResources()).thenReturn(result);
        ConfigResource cr = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, clientMetricsName);
        Config cfg = new Config(Collections.singleton(new ConfigEntry("metrics", "org.apache.kafka.producer.")));
        DescribeConfigsResult describeResult = AdminClientTestUtils.describeConfigsResult(cr, cfg);
        when(adminClient.describeConfigs(any())).thenReturn(describeResult);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.describeClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--describe"}));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertTrue(capturedOutput.contains("Client metrics configs for " + clientMetricsName + " are:"));
        assertTrue(capturedOutput.contains("metrics=org.apache.kafka.producer."));
    }

    @Test
    public void testList() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        ListClientMetricsResourcesResult result = AdminClientTestUtils.listClientMetricsResourcesResult("one", "two");
        when(adminClient.listClientMetricsResources()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.listClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--list"}));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertEquals("one,two", Arrays.stream(capturedOutput.split("\n")).collect(Collectors.joining(",")));
    }

    @Test
    public void testListFailsWithUnsupportedVersionException() {
        Admin adminClient = mock(Admin.class);
        ClientMetricsCommand.ClientMetricsService service = new ClientMetricsCommand.ClientMetricsService(adminClient);

        ListClientMetricsResourcesResult result = AdminClientTestUtils.listClientMetricsResourcesResult(Errors.UNSUPPORTED_VERSION.exception());
        when(adminClient.listClientMetricsResources()).thenReturn(result);

        assertThrows(ExecutionException.class,
                () -> service.listClientMetrics(new ClientMetricsCommand.ClientMetricsCommandOptions(
                            new String[] {"--bootstrap-server", bootstrapServer, "--list"})));
    }

    private void assertInitializeInvalidOptionsExitCode(int expected, String[] options) {
        Exit.setExitProcedure((exitCode, message) -> {
            assertEquals(expected, exitCode);
            throw new RuntimeException();
        });
        try {
            assertThrows(RuntimeException.class, () -> new ClientMetricsCommand.ClientMetricsCommandOptions(options));
        } finally {
            Exit.resetExitProcedure();
        }
    }
}
