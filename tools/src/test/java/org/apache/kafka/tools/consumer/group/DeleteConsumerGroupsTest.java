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
package org.apache.kafka.tools.consumer.group;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import joptsimple.OptionException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.GroupState.EMPTY;
import static org.apache.kafka.common.GroupState.STABLE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(value = ClusterTestExtensions.class)
public class DeleteConsumerGroupsTest {

    private static List<ClusterConfig> generator() {
        return ConsumerGroupCommandTestUtils.generator();
    }

    @Test
    public void testDeleteWithTopicOption() {
        String[] cgcArgs = new String[]{"--bootstrap-server", "localhost:62241", "--delete", "--group", getDummyGroupId(), "--topic"};
        assertThrows(OptionException.class, () -> ConsumerGroupCommandOptions.fromArgs(cgcArgs));
    }

    @ClusterTemplate("generator")
    public void testDeleteCmdNonExistingGroup(ClusterInstance cluster) {
        String missingGroupId = getDummyGroupId();
        String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", missingGroupId};
        try (ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs)) {
            String output = ToolsTestUtils.grabConsoleOutput(service::deleteGroups);
            assertTrue(output.contains("Group '" + missingGroupId + "' could not be deleted due to:") && output.contains(Errors.GROUP_ID_NOT_FOUND.message()),
                    "The expected error (" + Errors.GROUP_ID_NOT_FOUND + ") was not detected while deleting consumer group");
        }
    }

    @ClusterTemplate("generator")
    public void testDeleteNonExistingGroup(ClusterInstance cluster) {
        String missingGroupId = getDummyGroupId();
        String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", missingGroupId};
        try (ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs)) {
            Map<String, Throwable> result = service.deleteGroups();
            assertEquals(1, result.size());
            assertNotNull(result.get(missingGroupId));
            assertInstanceOf(GroupIdNotFoundException.class,
                    result.get(missingGroupId).getCause(),
                    "The expected error (" + Errors.GROUP_ID_NOT_FOUND + ") was not detected while deleting consumer group");
        }
    }

    @ClusterTemplate("generator")
    public void testDeleteNonEmptyGroup(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String groupId = composeGroupId(groupProtocol);
            String topicName = composeTopicName(groupProtocol);
            String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", groupId};
            try (
                    AutoCloseable consumerGroupCloseable = consumerGroupClosable(cluster, groupProtocol, groupId, topicName);
                    ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs)
            ) {
                TestUtils.waitForCondition(
                        () -> service.collectGroupMembers(groupId, false).getValue().get().size() == 1,
                        "The group did not initialize as expected."
                );

                String output = ToolsTestUtils.grabConsoleOutput(service::deleteGroups);
                Map<String, Throwable> result = service.deleteGroups();

                assertTrue(output.contains("Group '" + groupId + "' could not be deleted due to:") && output.contains(Errors.NON_EMPTY_GROUP.message()),
                        "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting consumer group. Output was: (" + output + ")");

                assertNotNull(result.get(groupId),
                        "Group was deleted successfully, but it shouldn't have been. Result was:(" + result + ")");

                assertEquals(1, result.size());
                assertNotNull(result.get(groupId));
                assertInstanceOf(GroupNotEmptyException.class,
                        result.get(groupId).getCause(),
                        "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting consumer group. Result was:(" + result + ")");
            }
        }
    }

    @ClusterTemplate("generator")
    void testDeleteEmptyGroup(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String groupId = composeGroupId(groupProtocol);
            String topicName = composeTopicName(groupProtocol);
            String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", groupId};
            try (
                    AutoCloseable consumerGroupCloseable = consumerGroupClosable(cluster, groupProtocol, groupId, topicName);
                    ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs)
            ) {
                TestUtils.waitForCondition(
                        () -> service.listConsumerGroups().contains(groupId) && checkGroupState(service, groupId, STABLE),
                        "The group did not initialize as expected."
                );

                consumerGroupCloseable.close();

                TestUtils.waitForCondition(
                        () -> checkGroupState(service, groupId, EMPTY),
                        "The group did not become empty as expected."
                );

                Map<String, Throwable> result = new HashMap<>();
                String output = ToolsTestUtils.grabConsoleOutput(() -> result.putAll(service.deleteGroups()));

                assertTrue(output.contains("Deletion of requested consumer groups ('" + groupId + "') was successful."),
                        "The consumer group could not be deleted as expected");
                assertEquals(1, result.size());
                assertTrue(result.containsKey(groupId));
                assertNull(result.get(groupId), "The consumer group could not be deleted as expected");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDeleteCmdAllGroups(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String topicName = composeTopicName(groupProtocol);
            // Create 3 groups with 1 consumer each
            Map<String, AutoCloseable> groupIdToExecutor = IntStream.rangeClosed(1, 3)
                    .mapToObj(i -> composeGroupId(groupProtocol) + i)
                    .collect(Collectors.toMap(Function.identity(), group -> consumerGroupClosable(cluster, groupProtocol, group, topicName)));
            String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--all-groups"};

            try (ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs)) {
                TestUtils.waitForCondition(() ->
                                new HashSet<>(service.listConsumerGroups()).equals(groupIdToExecutor.keySet()) &&
                                        groupIdToExecutor.keySet().stream().allMatch(groupId -> assertDoesNotThrow(() -> checkGroupState(service, groupId, STABLE))),
                        "The group did not initialize as expected.");

                // Shutdown consumers to empty out groups
                for (AutoCloseable consumerGroupExecutor : groupIdToExecutor.values()) {
                    consumerGroupExecutor.close();
                }

                TestUtils.waitForCondition(() ->
                                groupIdToExecutor.keySet().stream().allMatch(groupId -> assertDoesNotThrow(() -> checkGroupState(service, groupId, EMPTY))),
                        "The group did not become empty as expected.");

                String output = ToolsTestUtils.grabConsoleOutput(service::deleteGroups).trim();
                Set<String> expectedGroupsForDeletion = groupIdToExecutor.keySet();
                Set<String> deletedGroupsGrepped = Arrays.stream(output.substring(output.indexOf('(') + 1, output.indexOf(')')).split(","))
                        .map(str -> str.replaceAll("'", "").trim())
                        .collect(Collectors.toSet());

                assertTrue(output.matches("Deletion of requested consumer groups (.*) was successful.")
                                && Objects.equals(deletedGroupsGrepped, expectedGroupsForDeletion),
                        "The consumer group(s) could not be deleted as expected");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDeleteCmdWithMixOfSuccessAndError(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String groupId = composeGroupId(groupProtocol);
            String topicName = composeTopicName(groupProtocol);
            String missingGroupId = composeMissingGroupId(groupProtocol);
            String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", groupId};
            try (
                    AutoCloseable consumerGroupClosable = consumerGroupClosable(cluster, groupProtocol, groupId, topicName);
                    ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs)
            ) {
                TestUtils.waitForCondition(
                        () -> service.listConsumerGroups().contains(groupId) && checkGroupState(service, groupId, STABLE),
                        "The group did not initialize as expected.");

                consumerGroupClosable.close();
                TestUtils.waitForCondition(
                        () -> checkGroupState(service, groupId, EMPTY),
                        "The group did not become empty as expected.");

                cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", groupId, "--group", missingGroupId};

                try (ConsumerGroupCommand.ConsumerGroupService service2 = getConsumerGroupService(cgcArgs)) {
                    String output = ToolsTestUtils.grabConsoleOutput(service2::deleteGroups);
                    assertTrue(output.contains("Group '" + missingGroupId + "' could not be deleted due to:")
                                    && output.contains(Errors.GROUP_ID_NOT_FOUND.message())
                                    && output.contains("These consumer groups were deleted successfully: '" + groupId + "'"),
                            "The consumer group deletion did not work as expected");
                }
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDeleteWithMixOfSuccessAndError(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String groupId = composeGroupId(groupProtocol);
            String topicName = composeTopicName(groupProtocol);
            String missingGroupId = composeMissingGroupId(groupProtocol);
            String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", groupId};
            try (
                    AutoCloseable executor = consumerGroupClosable(cluster, groupProtocol, groupId, topicName);
                    ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs)
            ) {
                TestUtils.waitForCondition(
                        () -> service.listConsumerGroups().contains(groupId) && checkGroupState(service, groupId, STABLE),
                        "The group did not initialize as expected.");

                executor.close();

                TestUtils.waitForCondition(
                        () -> checkGroupState(service, groupId, EMPTY),
                        "The group did not become empty as expected.");

                cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", groupId, "--group", missingGroupId};

                try (ConsumerGroupCommand.ConsumerGroupService service2 = getConsumerGroupService(cgcArgs)) {
                    Map<String, Throwable> result = service2.deleteGroups();
                    assertTrue(result.size() == 2 &&
                                    result.containsKey(groupId) && result.get(groupId) == null &&
                                    result.containsKey(missingGroupId) &&
                                    result.get(missingGroupId).getMessage().contains(Errors.GROUP_ID_NOT_FOUND.message()),
                            "The consumer group deletion did not work as expected");
                }
            }
        }
    }

    @Test
    public void testDeleteWithUnrecognizedNewConsumerOption() {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", "localhost:62241", "--delete", "--group", getDummyGroupId()};
        assertThrows(OptionException.class, () -> ConsumerGroupCommandOptions.fromArgs(cgcArgs));
    }

    private String getDummyGroupId() {
        return composeGroupId(null);
    }

    private String composeGroupId(GroupProtocol protocol) {
        String groupPrefix = "test.";
        return protocol != null ? groupPrefix + protocol.name : groupPrefix + "dummy";
    }

    private String composeTopicName(GroupProtocol protocol) {
        String topicPrefix = "foo.";
        return protocol != null ? topicPrefix + protocol.name : topicPrefix + "dummy";
    }

    private String composeMissingGroupId(GroupProtocol protocol) {
        String missingGroupPrefix = "missing.";
        return protocol != null ? missingGroupPrefix + protocol.name : missingGroupPrefix + "dummy";
    }

    private AutoCloseable consumerGroupClosable(ClusterInstance cluster, GroupProtocol protocol, String groupId, String topicName) {
        Map<String, Object> configs = composeConfigs(
                cluster,
                groupId,
                protocol.name,
                emptyMap());

        return ConsumerGroupCommandTestUtils.buildConsumers(
                1,
                false,
                topicName,
                () -> new KafkaConsumer<String, String>(configs)
        );
    }

    private boolean checkGroupState(ConsumerGroupCommand.ConsumerGroupService service, String groupId, GroupState state) throws Exception {
        return Objects.equals(service.collectGroupState(groupId).groupState, state);
    }

    private ConsumerGroupCommand.ConsumerGroupService getConsumerGroupService(String[] args) {
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(args);
        return new ConsumerGroupCommand.ConsumerGroupService(
                opts,
                singletonMap(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    private Map<String, Object> composeConfigs(ClusterInstance cluster, String groupId, String groupProtocol, Map<String, Object> customConfigs) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        configs.put(GROUP_ID_CONFIG, groupId);
        configs.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(GROUP_PROTOCOL_CONFIG, groupProtocol);
        if (GroupProtocol.CLASSIC.name.equalsIgnoreCase(groupProtocol)) {
            configs.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        }
        configs.putAll(customConfigs);
        return configs;
    }
}
