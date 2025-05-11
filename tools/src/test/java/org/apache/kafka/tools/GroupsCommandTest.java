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
import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(value = 60)
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
    public void testOptionsListShareFilterSucceeds() {
        GroupsCommand.GroupsCommandOptions opts = new GroupsCommand.GroupsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--list", "--share"});
        assertTrue(opts.hasListOption());
        assertTrue(opts.hasShareOption());
    }
    
    @Test
    public void testOptionsListStreamsFilterSucceeds() {
        GroupsCommand.GroupsCommandOptions opts = new GroupsCommand.GroupsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--list", "--streams"});
        assertTrue(opts.hasListOption());
        assertTrue(opts.hasStreamsOption());
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
    public void testOptionsListTypeStreamsFilterSucceeds() {
        GroupsCommand.GroupsCommandOptions opts = new GroupsCommand.GroupsCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer, "--list", "--group-type", "streams"});
        assertTrue(opts.hasListOption());
        assertTrue(opts.groupType().isPresent());
        assertEquals(GroupType.STREAMS, opts.groupType().get());
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
    public void testOptionsListConsumerAndShareFilterFails() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer, "--list", "--consumer", "--share"});
    }

    @Test
    public void testOptionsListShareAndStreamsFilterFails() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer, "--list", "--share", "--streams"});
    }

    @Test
    public void testOptionsListConsumerAndStreamsFilterFails() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer, "--list", "--consumer", "--streams"});
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
    public void testOptionsListShareAndProtocolFilterFails() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer, "--list", "--share", "--protocol", "anyproto"});
    }

    @Test
    public void testOptionsListShareAndTypeFilterFails() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer, "--list", "--share", "--group-type", "classic"});
    }
    
    @Test
    public void testOptionsListStreamsAndProtocolFilterFails() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer, "--list", "--streams", "--protocol", "anyproto"});
    }

    @Test
    public void testOptionsListStreamsAndTypeFilterFails() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer, "--list", "--streams", "--group-type", "classic"});
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
        assertCapturedListOutput(capturedOutput);
    }

    @Test
    public void testListGroups() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", Optional.of(GroupType.CLASSIC), "consumer", Optional.of(GroupState.STABLE)),
                new GroupListing("CGconsumer", Optional.of(GroupType.CONSUMER), "consumer", Optional.of(GroupState.STABLE)),
                new GroupListing("SG", Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
                new GroupListing("StrG", Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE))
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
        assertCapturedListOutput(capturedOutput,
                new String[]{"CGclassic", "Classic", "consumer"},
                new String[]{"CGconsumer", "Consumer", "consumer"},
                new String[]{"SG", "Share", "share"},
                new String[]{"StrG", "Streams", "streams"});
    }

    @Test
    public void testListGroupsConsumerFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", Optional.of(GroupType.CLASSIC), "consumer", Optional.of(GroupState.STABLE)),
                new GroupListing("CGconsumer", Optional.of(GroupType.CONSUMER), "consumer", Optional.of(GroupState.STABLE)),
                new GroupListing("SG", Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
                new GroupListing("StrG", Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE))
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
        assertCapturedListOutput(capturedOutput,
                new String[]{"CGclassic", "Classic", "consumer"},
                new String[]{"CGconsumer", "Consumer", "consumer"});
    }

    @Test
    public void testListGroupsShareFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
            new GroupListing("CGclassic", Optional.of(GroupType.CLASSIC), "consumer", Optional.of(GroupState.STABLE)),
            new GroupListing("CGconsumer", Optional.of(GroupType.CONSUMER), "consumer", Optional.of(GroupState.STABLE)),
            new GroupListing("SG", Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
            new GroupListing("StrG", Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE))
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.listGroups(new GroupsCommand.GroupsCommandOptions(
                    new String[]{"--bootstrap-server", bootstrapServer, "--list", "--share"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertCapturedListOutput(capturedOutput,
            new String[]{"SG", "Share", "share"});
    }

    @Test
    public void testListGroupsStreamsFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
            new GroupListing("CGclassic", Optional.of(GroupType.CLASSIC), "consumer", Optional.of(GroupState.STABLE)),
            new GroupListing("CGconsumer", Optional.of(GroupType.CONSUMER), "consumer", Optional.of(GroupState.STABLE)),
            new GroupListing("SG", Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
            new GroupListing("StrG", Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE))
        );
        when(adminClient.listGroups()).thenReturn(result);

        String capturedOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.listGroups(new GroupsCommand.GroupsCommandOptions(
                    new String[]{"--bootstrap-server", bootstrapServer, "--list", "--streams"}
                ));
            } catch (Throwable t) {
                fail(t);
            }
        });
        assertCapturedListOutput(capturedOutput,
            new String[]{"StrG", "Streams", "streams"});
    }

    @Test
    public void testListGroupsProtocolFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", Optional.of(GroupType.CLASSIC), "consumer", Optional.of(GroupState.STABLE)),
                new GroupListing("CGconsumer", Optional.of(GroupType.CONSUMER), "consumer", Optional.of(GroupState.STABLE)),
                new GroupListing("SG", Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
                new GroupListing("StrG", Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE))
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
        assertCapturedListOutput(capturedOutput,
                new String[]{"CGclassic", "Classic", "consumer"},
                new String[]{"CGconsumer", "Consumer", "consumer"});
    }

    @Test
    public void testListGroupsTypeFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", Optional.of(GroupType.CLASSIC), "consumer", Optional.of(GroupState.STABLE)),
                new GroupListing("CGconsumer", Optional.of(GroupType.CONSUMER), "consumer", Optional.of(GroupState.STABLE)),
                new GroupListing("SG", Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
                new GroupListing("StrG", Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE))
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
        assertCapturedListOutput(capturedOutput,
                new String[]{"SG", "Share", "share"});
    }

    @Test
    public void testListGroupsProtocolAndTypeFilter() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGclassic", Optional.of(GroupType.CLASSIC), "consumer", Optional.of(GroupState.STABLE)),
                new GroupListing("CGconsumer", Optional.of(GroupType.CONSUMER), "consumer", Optional.of(GroupState.STABLE)),
                new GroupListing("SG", Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
                new GroupListing("StrG", Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE))
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
        assertCapturedListOutput(capturedOutput,
                new String[]{"CGclassic", "Classic", "consumer"});
    }

    @Test
    public void testListGroupsProtocolAndTypeFilterNoMatch() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(
                new GroupListing("CGconsumer", Optional.of(GroupType.CONSUMER), "consumer", Optional.of(GroupState.STABLE)),
                new GroupListing("SG", Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
                new GroupListing("StrG", Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE))
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
        assertCapturedListOutput(capturedOutput);
    }

    @Test
    public void testListGroupsFailsWithException() {
        Admin adminClient = mock(Admin.class);
        GroupsCommand.GroupsService service = new GroupsCommand.GroupsService(adminClient);

        ListGroupsResult result = AdminClientTestUtils.listGroupsResult(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        when(adminClient.listGroups()).thenReturn(result);

        assertThrows(ExecutionException.class, () -> service.listGroups(new GroupsCommand.GroupsCommandOptions(
            new String[]{"--bootstrap-server", bootstrapServer, "--list"}
        )));
    }

    @SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic,consumer,streams"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
        }
    )
    public void testGroupCommand(ClusterInstance clusterInstance) throws Exception {
        String topic = "topic";
        String outputTopic = "output-topic";
        String classicGroupId = "classic_group";
        String consumerGroupId = "consumer_group";
        String shareGroupId = "share_group";
        String streamsGroupId = "streams_group";
        String simpleGroupId = "simple_group";
        clusterInstance.createTopic(topic, 1, (short) 1);
        clusterInstance.createTopic(outputTopic, 1, (short) 1);
        TopicPartition topicPartition = new TopicPartition(topic, 0);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());

        try (KafkaConsumer<String, String> classicGroup = createKafkaConsumer(clusterInstance, classicGroupId, GroupProtocol.CLASSIC);
             KafkaConsumer<String, String> consumerGroup = createKafkaConsumer(clusterInstance, consumerGroupId, GroupProtocol.CONSUMER);
             KafkaShareConsumer<String, String> shareGroup = createKafkaShareConsumer(clusterInstance, shareGroupId);
             KafkaStreams streams = createKafkaStreams(clusterInstance, streamsGroupId, topic, outputTopic);
             Admin admin = clusterInstance.admin();
             GroupsCommand.GroupsService groupsCommand = new GroupsCommand.GroupsService(props)
        ) {
            classicGroup.subscribe(List.of(topic));
            classicGroup.poll(Duration.ofMillis(1000));
            consumerGroup.subscribe(List.of(topic));
            consumerGroup.poll(Duration.ofMillis(1000));
            shareGroup.subscribe(List.of(topic));
            shareGroup.poll(Duration.ofMillis(1000));
            streams.start();

            AlterConsumerGroupOffsetsResult result = admin.alterConsumerGroupOffsets(simpleGroupId, Map.of(topicPartition, new OffsetAndMetadata(0L)));
            assertNull(result.all().get());

            TestUtils.waitForCondition(() -> {
                Map.Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(() ->
                    assertDoesNotThrow(() -> groupsCommand.listGroups(new GroupsCommand.GroupsCommandOptions(
                        List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list").toArray(new String[0])))));
                if (res.getKey().split("\n").length == 6 && res.getValue().isEmpty()) {
                    assertCapturedListOutput(res.getKey(),
                        new String[]{classicGroupId, "Classic", "consumer"},
                        new String[]{consumerGroupId, "Consumer", "consumer"},
                        new String[]{simpleGroupId, "Classic"},
                        new String[]{shareGroupId, "Share", "share"},
                        new String[]{streamsGroupId, "Streams", "streams"});
                    return true;
                }
                return false;
            }, "Waiting for listing groups to return all groups");

            TestUtils.waitForCondition(() -> {
                Map.Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(() ->
                    assertDoesNotThrow(() -> groupsCommand.listGroups(new GroupsCommand.GroupsCommandOptions(
                        List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--consumer").toArray(new String[0])))));
                if (res.getKey().split("\n").length == 4 && res.getValue().isEmpty()) {
                    assertCapturedListOutput(res.getKey(),
                        new String[]{classicGroupId, "Classic", "consumer"},
                        new String[]{consumerGroupId, "Consumer", "consumer"},
                        new String[]{simpleGroupId, "Classic"});
                    return true;
                }
                return false;
            }, "Waiting for listing groups to return consumer protocol groups");

            TestUtils.waitForCondition(() -> {
                Map.Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(() ->
                    assertDoesNotThrow(() -> groupsCommand.listGroups(new GroupsCommand.GroupsCommandOptions(
                        List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--group-type", "classic").toArray(new String[0])))));
                if (res.getKey().split("\n").length == 3 && res.getValue().isEmpty()) {
                    assertCapturedListOutput(res.getKey(),
                        new String[]{classicGroupId, "Classic", "consumer"},
                        new String[]{simpleGroupId, "Classic"});
                    return true;
                }
                return false;
            }, "Waiting for listing groups to return classic type groups");

            TestUtils.waitForCondition(() -> {
                Map.Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(() ->
                    assertDoesNotThrow(() -> groupsCommand.listGroups(new GroupsCommand.GroupsCommandOptions(
                        List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--group-type", "consumer").toArray(new String[0])))));
                if (res.getKey().split("\n").length == 2 && res.getValue().isEmpty()) {
                    assertCapturedListOutput(res.getKey(),
                        new String[]{consumerGroupId, "Consumer", "consumer"});
                    return true;
                }
                return false;
            }, "Waiting for listing groups to return consumer type groups");

            TestUtils.waitForCondition(() -> {
                Map.Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(() ->
                    assertDoesNotThrow(() -> groupsCommand.listGroups(new GroupsCommand.GroupsCommandOptions(
                        List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--group-type", "share").toArray(new String[0])))));
                if (res.getKey().split("\n").length == 2 && res.getValue().isEmpty()) {
                    assertCapturedListOutput(res.getKey(),
                        new String[]{shareGroupId, "Share", "share"});
                    return true;
                }
                return false;
            }, "Waiting for listing groups to return share type groups");

            TestUtils.waitForCondition(() -> {
                Map.Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(() ->
                    assertDoesNotThrow(() -> groupsCommand.listGroups(new GroupsCommand.GroupsCommandOptions(
                        List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--group-type", "streams").toArray(new String[0])))));
                if (res.getKey().split("\n").length == 2 && res.getValue().isEmpty()) {
                    assertCapturedListOutput(res.getKey(),
                        new String[]{streamsGroupId, "Streams", "streams"});
                    return true;
                }
                return false;
            }, "Waiting for listing groups to return streams type groups");

            TestUtils.waitForCondition(() -> {
                Map.Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(() ->
                    assertDoesNotThrow(() -> groupsCommand.listGroups(new GroupsCommand.GroupsCommandOptions(
                        List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--share").toArray(new String[0])))));
                if (res.getKey().split("\n").length == 2 && res.getValue().isEmpty()) {
                    assertCapturedListOutput(res.getKey(),
                        new String[]{shareGroupId, "Share", "share"});
                    return true;
                }
                return false;
            }, "Waiting for listing groups to return share type groups");


            TestUtils.waitForCondition(() -> {
                Map.Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(() ->
                    assertDoesNotThrow(() -> groupsCommand.listGroups(new GroupsCommand.GroupsCommandOptions(
                        List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--streams").toArray(new String[0])))));
                if (res.getKey().split("\n").length == 2 && res.getValue().isEmpty()) {
                    assertCapturedListOutput(res.getKey(),
                        new String[]{streamsGroupId, "Streams", "streams"});
                    return true;
                }
                return false;
            }, "Waiting for listing groups to return streams type groups");
        }
    }

    private void assertInitializeInvalidOptionsExitCode(int expected, String[] options) {
        Exit.setExitProcedure((exitCode, message) -> {
            assertEquals(expected, exitCode);
            throw new RuntimeException();
        });
        try {
            assertThrows(RuntimeException.class, () -> new GroupsCommand.GroupsCommandOptions(options));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    private void assertCapturedListOutput(String capturedOutput, String[]... expectedLines) {
        String[] capturedLines = capturedOutput.split("\n");
        assertEquals(expectedLines.length + 1, capturedLines.length);
        assertEquals("GROUP,TYPE,PROTOCOL", String.join(",", capturedLines[0].split(" +")));
        int i = 1;
        for (String[] line : expectedLines) {
            assertEquals(String.join(",", line), String.join(",", capturedLines[i++].split(" +")));
        }
    }

    private KafkaConsumer<String, String> createKafkaConsumer(ClusterInstance clusterInstance, String groupId, GroupProtocol groupProtocol) {
        return new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));
    }

    private KafkaShareConsumer<String, String> createKafkaShareConsumer(ClusterInstance clusterInstance, String groupId) {
        return new KafkaShareConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));
    }

    private KafkaStreams createKafkaStreams(ClusterInstance clusterInstance, String groupId, String inputTopic, String outputTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, String> input = builder.stream(inputTopic);
        input.map((key, value) -> new KeyValue<>(key, key)).to(outputTopic, Produced.with(Serdes.Long(), Serdes.Long()));
        Topology topology = builder.build();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, groupId);
        properties.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, "streams");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return new KafkaStreams(topology, properties);
    }
}
