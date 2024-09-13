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
import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Exit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GroupsCommandTest {

    private final String bootstrapServer = "localhost:9092";
    private final ToolsTestUtils.MockExitProcedure exitProcedure = new ToolsTestUtils.MockExitProcedure();

    @BeforeEach
    public void setupExitProcedure() {
        Exit.setExitProcedure(exitProcedure);
    }

    @AfterEach
    public void resetExitProcedure() {
        Exit.resetExitProcedure();
    }

    @Test
    public void testOptionsNoActionFails() {
        assertInitializeInvalidOptionsExitCode(1,
                new String[] {"--bootstrap-server", bootstrapServer});
    }

    @Test
    public void testOptionsListSucceeds() {
        GroupsCommand.GroupsCommandOptions opts = new GroupsCommand.GroupsCommandOptions(
                new String[] {"--bootstrap-server", bootstrapServer, "--list"});
        assertTrue(opts.hasListOption());
    }

    @Test
    public void testOptionsListConsumerFilterSucceeds() {
        GroupsCommand.GroupsCommandOptions opts = new GroupsCommand.GroupsCommandOptions(
                new String[] {"--bootstrap-server", bootstrapServer, "--list", "--consumer"});
        assertTrue(opts.hasListOption());
        assertTrue(opts.hasConsumerOption());
    }

    @Test
    public void testOptionsListProtocolFilterSucceeds() {
        GroupsCommand.GroupsCommandOptions opts = new GroupsCommand.GroupsCommandOptions(
                new String[] {"--bootstrap-server", bootstrapServer, "--list", "--protocol", "anyproto"});
        assertTrue(opts.hasListOption());
        assertTrue(opts.protocol().isPresent());
        assertEquals("anyproto", opts.protocol().get());
    }

    @Test
    public void testOptionsListTypeFilterSucceeds() {
        GroupsCommand.GroupsCommandOptions opts = new GroupsCommand.GroupsCommandOptions(
                new String[] {"--bootstrap-server", bootstrapServer, "--list", "--group-type", "share"});
        assertTrue(opts.hasListOption());
        assertTrue(opts.groupType().isPresent());
        assertEquals(GroupType.SHARE, opts.groupType().get());
    }

    @Test
    public void testOptionsListInvalidTypeFilterFails() {
        assertInitializeInvalidOptionsExitCode(1,
                new String[] {"--bootstrap-server", bootstrapServer, "--list", "--group-type", "invalid"});
    }

    @Test
    public void testOptionsListProtocolAndTypeFiltersSucceeds() {
        GroupsCommand.GroupsCommandOptions opts = new GroupsCommand.GroupsCommandOptions(
                new String[] {"--bootstrap-server", bootstrapServer, "--list", "--protocol", "anyproto", "--group-type", "share"});
        assertTrue(opts.hasListOption());
        assertTrue(opts.protocol().isPresent());
        assertEquals("anyproto", opts.protocol().get());
        assertTrue(opts.groupType().isPresent());
        assertEquals(GroupType.SHARE, opts.groupType().get());
    }

    @Test
    public void testOptionsListConsumerAndProtocolFilterFails() {
        assertInitializeInvalidOptionsExitCode(1,
                new String[] {"--bootstrap-server", bootstrapServer, "--list", "--consumer", "--protocol", "anyproto"});
    }

    @Test
    public void testOptionsListConsumerAndTypeFilterFails() {
        assertInitializeInvalidOptionsExitCode(1,
                new String[] {"--bootstrap-server", bootstrapServer, "--list", "--consumer", "--group-type", "share"});
    }

    @Test
    public void testOptionsDescribeSucceeds() {
        GroupsCommand.GroupsCommandOptions opts = new GroupsCommand.GroupsCommandOptions(
                new String[] {"--bootstrap-server", bootstrapServer, "--describe"});
        assertTrue(opts.hasDescribeOption());
    }

    @Test
    public void testOptionsDescribeConsumerFilterSucceeds() {
        GroupsCommand.GroupsCommandOptions opts = new GroupsCommand.GroupsCommandOptions(
                new String[] {"--bootstrap-server", bootstrapServer, "--describe", "--consumer"});
        assertTrue(opts.hasDescribeOption());
        assertTrue(opts.hasConsumerOption());
    }

    @Test
    public void testOptionsDescribeProtocolFilterSucceeds() {
        GroupsCommand.GroupsCommandOptions opts = new GroupsCommand.GroupsCommandOptions(
                new String[] {"--bootstrap-server", bootstrapServer, "--describe", "--protocol", "anyproto"});
        assertTrue(opts.hasDescribeOption());
        assertTrue(opts.protocol().isPresent());
        assertEquals("anyproto", opts.protocol().get());
    }

    @Test
    public void testOptionsDescribeTypeFilterSucceeds() {
        GroupsCommand.GroupsCommandOptions opts = new GroupsCommand.GroupsCommandOptions(
                new String[] {"--bootstrap-server", bootstrapServer, "--describe", "--group-type", "share"});
        assertTrue(opts.hasDescribeOption());
        assertTrue(opts.groupType().isPresent());
        assertEquals(GroupType.SHARE, opts.groupType().get());
    }

    @Test
    public void testOptionsDescribeInvalidTypeFilterFails() {
        assertInitializeInvalidOptionsExitCode(1,
                new String[] {"--bootstrap-server", bootstrapServer, "--describe", "--group-type", "invalid"});
    }

    @Test
    public void testOptionsDescribeProtocolAndTypeFiltersSucceeds() {
        GroupsCommand.GroupsCommandOptions opts = new GroupsCommand.GroupsCommandOptions(
                new String[] {"--bootstrap-server", bootstrapServer, "--describe", "--protocol", "anyproto", "--group-type", "share"});
        assertTrue(opts.hasDescribeOption());
        assertTrue(opts.protocol().isPresent());
        assertEquals("anyproto", opts.protocol().get());
        assertTrue(opts.groupType().isPresent());
        assertEquals(GroupType.SHARE, opts.groupType().get());
    }

    @Test
    public void testOptionsDescribeConsumerAndProtocolFilterFails() {
        assertInitializeInvalidOptionsExitCode(1,
                new String[] {"--bootstrap-server", bootstrapServer, "--describe", "--consumer", "--protocol", "anyproto"});
    }

    @Test
    public void testOptionsDescribeConsumerAndTypeFilterFails() {
        assertInitializeInvalidOptionsExitCode(1,
                new String[] {"--bootstrap-server", bootstrapServer, "--describe", "--consumer", "--group-type", "share"});
    }

    @Test
    public void testListGroupsEmpty() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult();
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.listGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--list"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertEquals("", capturedOutput);
    }

    @Test
    public void testListGroups() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", GroupType.CLASSIC, "consumer"),
                new GroupListing("CGconsumer", GroupType.CONSUMER, "consumer"),
                new GroupListing("SG", GroupType.SHARE, "share")
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.listGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--list"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertEquals("CGclassic,CGconsumer,SG", String.join(",", capturedOutput.split("\n")));
    }

    @Test
    public void testListGroupsConsumerFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", GroupType.CLASSIC, "consumer"),
                new GroupListing("CGconsumer", GroupType.CONSUMER, "consumer"),
                new GroupListing("SG", GroupType.SHARE, "share")
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.listGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--list", "--consumer"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertEquals("CGclassic,CGconsumer", String.join(",", capturedOutput.split("\n")));
    }

    @Test
    public void testListGroupsConsumerFilterSimpleCG() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", GroupType.CLASSIC, "consumer"),
                new GroupListing("CGconsumer", GroupType.CONSUMER, "consumer"),
                new GroupListing("CGsimple", GroupType.CLASSIC, ""),
                new GroupListing("SG", GroupType.SHARE, "share")
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.listGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--list", "--consumer"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertEquals("CGclassic,CGconsumer,CGsimple", String.join(",", capturedOutput.split("\n")));
    }

    @Test
    public void testListGroupsProtocolFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", GroupType.CLASSIC, "consumer"),
                new GroupListing("CGconsumer", GroupType.CONSUMER, "consumer"),
                new GroupListing("SG", GroupType.SHARE, "share")
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.listGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--list", "--protocol", "consumer"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertEquals("CGclassic,CGconsumer", String.join(",", capturedOutput.split("\n")));
    }

    @Test
    public void testListGroupsTypeFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", GroupType.CLASSIC, "consumer"),
                new GroupListing("CGconsumer", GroupType.CONSUMER, "consumer"),
                new GroupListing("SG", GroupType.SHARE, "share")
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.listGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--list", "--group-type", "share"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertEquals("SG", String.join(",", capturedOutput.split("\n")));
    }

    @Test
    public void testListGroupsProtocolAndTypeFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", GroupType.CLASSIC, "consumer"),
                new GroupListing("CGconsumer", GroupType.CONSUMER, "consumer"),
                new GroupListing("SG", GroupType.SHARE, "share")
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.listGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--list", "--protocol", "consumer", "--group-type", "classic"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertEquals("CGclassic", String.join(",", capturedOutput.split("\n")));
    }

    @Test
    public void testListGroupsProtocolAndTypeFilterNoMatch() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGconsumer", GroupType.CONSUMER, "consumer"),
                new GroupListing("SG", GroupType.SHARE, "share")
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.listGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--list", "--protocol", "consumer", "--group-type", "classic"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertEquals("", capturedOutput);
    }

    @Test
    public void testDescribeGroupsEmpty() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult();
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.describeGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--describe"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertCapturedDescribeOutput(capturedOutput);
    }

    @Test
    public void testDescribeGroups() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", GroupType.CLASSIC, "consumer"),
                new GroupListing("CGconsumer", GroupType.CONSUMER, "consumer"),
                new GroupListing("SG", GroupType.SHARE, "share")
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.describeGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--describe"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertCapturedDescribeOutput(capturedOutput,
                new String[]{"CGclassic", "Classic", "consumer"},
                new String[]{"CGconsumer", "Consumer", "consumer"},
                new String[]{"SG", "Share", "share"});
    }

    @Test
    public void testDescribeGroupsConsumerFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", GroupType.CLASSIC, "consumer"),
                new GroupListing("CGconsumer", GroupType.CONSUMER, "consumer"),
                new GroupListing("SG", GroupType.SHARE, "share")
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.describeGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--describe", "--consumer"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertCapturedDescribeOutput(capturedOutput,
                new String[]{"CGclassic", "Classic", "consumer"},
                new String[]{"CGconsumer", "Consumer", "consumer"});
    }

    @Test
    public void testDescribeGroupsProtocolFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", GroupType.CLASSIC, "consumer"),
                new GroupListing("CGconsumer", GroupType.CONSUMER, "consumer"),
                new GroupListing("SG", GroupType.SHARE, "share")
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.describeGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--describe", "--protocol", "consumer"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertCapturedDescribeOutput(capturedOutput,
                new String[]{"CGclassic", "Classic", "consumer"},
                new String[]{"CGconsumer", "Consumer", "consumer"});
    }

    @Test
    public void testDescribeGroupsTypeFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", GroupType.CLASSIC, "consumer"),
                new GroupListing("CGconsumer", GroupType.CONSUMER, "consumer"),
                new GroupListing("SG", GroupType.SHARE, "share")
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.describeGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--describe", "--group-type", "share"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertCapturedDescribeOutput(capturedOutput,
                new String[]{"SG", "Share", "share"});
    }

    @Test
    public void testDescribeGroupsProtocolAndTypeFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", GroupType.CLASSIC, "consumer"),
                new GroupListing("CGconsumer", GroupType.CONSUMER, "consumer"),
                new GroupListing("SG", GroupType.SHARE, "share")
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.describeGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--describe", "--protocol", "consumer", "--group-type", "classic"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertCapturedDescribeOutput(capturedOutput,
                new String[]{"CGclassic", "Classic", "consumer"});
    }

    @Test
    public void testDescribeGroupsProtocolAndTypeFilterNoMatch() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGconsumer", GroupType.CONSUMER, "consumer"),
                new GroupListing("SG", GroupType.SHARE, "share")
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.describeGroups(new GroupsCommand.GroupsCommandOptions(
                        new String[]{"--bootstrap-server", bootstrapServer, "--describe", "--protocol", "consumer", "--group-type", "classic"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertCapturedDescribeOutput(capturedOutput);
    }

    @Test
    public void testDescribeGroupsFailsWithException() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        when(adminClient.listGroups()).thenReturn(result);

        assertThrows(ExecutionException.class, () -> service.describeGroups(new GroupsCommand.GroupsCommandOptions(
            new String[]{"--bootstrap-server", bootstrapServer, "--describe"}
        )));
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

    private void assertCapturedDescribeOutput(String capturedOutput, String[]... expectedLines) {
        String[] capturedLines = capturedOutput.split("\n");
        assertEquals(expectedLines.length + 1, capturedLines.length);
        assertEquals("GROUP,TYPE,PROTOCOL", String.join(",", capturedLines[0].split(" +")));
        int i = 1;
        for (String[] line : expectedLines) {
            assertEquals(String.join(",", line), String.join(",", capturedLines[i++].split(" +")));
        }
    }
}
