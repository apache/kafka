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

import joptsimple.OptionException;
import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.consumer.GroupProtocol.CLASSIC;
import static org.apache.kafka.clients.consumer.GroupProtocol.CONSUMER;
import static org.apache.kafka.common.ConsumerGroupState.EMPTY;
import static org.apache.kafka.common.ConsumerGroupState.STABLE;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(clusterType = Type.ALL, serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true")
})
public class DeleteConsumerGroupsTest {
    private static final String TOPIC = "foo";
    private static final String GROUP = "test.group";
    private static final String MISSING_GROUP = "missing.group";

    private final ClusterInstance cluster;
    private final Iterable<GroupProtocol> groupProtocols;

    public DeleteConsumerGroupsTest(ClusterInstance cluster) {
        this.cluster = cluster;
        this.groupProtocols = cluster.isKRaftTest()
                ? Arrays.asList(CLASSIC, CONSUMER)
                : singleton(CLASSIC);
    }

    @ClusterTest
    public void testDeleteWithTopicOption() {
        String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP, "--topic"};
        assertThrows(OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    @ClusterTest
    public void testDeleteCmdNonExistingGroup() {
        String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", MISSING_GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);
        String output = ToolsTestUtils.grabConsoleOutput(service::deleteGroups);
        assertTrue(output.contains("Group '" + MISSING_GROUP + "' could not be deleted due to:") && output.contains(Errors.GROUP_ID_NOT_FOUND.message()),
                "The expected error (" + Errors.GROUP_ID_NOT_FOUND + ") was not detected while deleting consumer group");
    }

    @ClusterTest
    public void testDeleteNonExistingGroup() {
        String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", MISSING_GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);
        Map<String, Throwable> result = service.deleteGroups();
        assertTrue(result.size() == 1 && result.containsKey(MISSING_GROUP) && result.get(MISSING_GROUP).getCause() instanceof GroupIdNotFoundException,
                "The expected error (" + Errors.GROUP_ID_NOT_FOUND + ") was not detected while deleting consumer group");
    }

    @ClusterTest
    public void testDeleteNonEmptyGroup() throws Exception {
        for (GroupProtocol groupProtocol : groupProtocols) {
            try (ConsumerGroupExecutor consumerGroupExecutor = buildConsumerGroupExecutor(GROUP, groupProtocol)) {
                String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP};
                ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);
                TestUtils.waitForCondition(
                        () -> service.collectGroupMembers(GROUP, false).getValue().get().size() == 1,
                        "The group did not initialize as expected."
                );

                String output = ToolsTestUtils.grabConsoleOutput(service::deleteGroups);
                Map<String, Throwable> result = service.deleteGroups();

                assertTrue(output.contains("Group '" + GROUP + "' could not be deleted due to:") && output.contains(Errors.NON_EMPTY_GROUP.message()),
                        "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting consumer group. Output was: (" + output + ")");

                assertNotNull(result.get(GROUP),
                        "Group was deleted successfully, but it shouldn't have been. Result was:(" + result + ")");

                assertTrue(result.size() == 1 && result.containsKey(GROUP) && result.get(GROUP).getCause() instanceof GroupNotEmptyException,
                        "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting consumer group. Result was:(" + result + ")");
            }
        }
    }

    @ClusterTest
    void testDeleteEmptyGroup() throws Exception {
        for (GroupProtocol groupProtocol : groupProtocols) {
            try (ConsumerGroupExecutor consumerGroupExecutor = buildConsumerGroupExecutor(GROUP, groupProtocol)) {
                String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP};
                ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

                TestUtils.waitForCondition(
                        () -> service.listConsumerGroups().contains(GROUP) && checkGroupState(service, GROUP, STABLE),
                        "The group did not initialize as expected."
                );

                consumerGroupExecutor.close();

                TestUtils.waitForCondition(
                        () -> checkGroupState(service, GROUP, EMPTY),
                        "The group did not become empty as expected."
                );

                Map<String, Throwable> result = new HashMap<>();
                String output = ToolsTestUtils.grabConsoleOutput(() -> result.putAll(service.deleteGroups()));

                assertTrue(output.contains("Deletion of requested consumer groups ('" + GROUP + "') was successful."),
                        "The consumer group could not be deleted as expected");
                assertTrue(result.size() == 1 && result.containsKey(GROUP) && result.get(GROUP) == null,
                        "The consumer group could not be deleted as expected");
            }
        }
    }

    @ClusterTest
    public void testDeleteCmdAllGroups() throws Exception {
        for (GroupProtocol groupProtocol : groupProtocols) {
            // Create 3 groups with 1 consumer per each
            Map<String, ConsumerGroupExecutor> groupNameToExecutor = IntStream.rangeClosed(1, 3)
                    .mapToObj(i -> GROUP + i)
                    .collect(Collectors.toMap(Function.identity(), group -> buildConsumerGroupExecutor(group, groupProtocol)));

            String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--all-groups"};
            ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

            TestUtils.waitForCondition(() ->
                            new HashSet<>(service.listConsumerGroups()).equals(groupNameToExecutor.keySet()) &&
                                    groupNameToExecutor.keySet().stream().allMatch(predicateGroupState(service, STABLE)),
                    "The group did not initialize as expected.");

            // Shutdown consumers to empty out groups
            for (ConsumerGroupExecutor consumerGroupExecutor : groupNameToExecutor.values()) {
                consumerGroupExecutor.close();
            }

            TestUtils.waitForCondition(() ->
                            groupNameToExecutor.keySet().stream().allMatch(predicateGroupState(service, EMPTY)),
                    "The group did not become empty as expected.");

            String output = ToolsTestUtils.grabConsoleOutput(service::deleteGroups).trim();
            Set<String> expectedGroupsForDeletion = groupNameToExecutor.keySet();
            Set<String> deletedGroupsGrepped = Arrays.stream(output.substring(output.indexOf('(') + 1, output.indexOf(')')).split(","))
                    .map(str -> str.replaceAll("'", "").trim())
                    .collect(Collectors.toSet());

            assertTrue(output.matches("Deletion of requested consumer groups (.*) was successful.")
                            && Objects.equals(deletedGroupsGrepped, expectedGroupsForDeletion),
                    "The consumer group(s) could not be deleted as expected");
        }
    }

    @ClusterTest
    public void testDeleteCmdWithMixOfSuccessAndError() throws Exception {
        for (GroupProtocol groupProtocol : groupProtocols) {
            try (ConsumerGroupExecutor executor = buildConsumerGroupExecutor(GROUP, groupProtocol)) {
                String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP};
                ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

                TestUtils.waitForCondition(
                        () -> service.listConsumerGroups().contains(GROUP) && checkGroupState(service, GROUP, STABLE),
                        "The group did not initialize as expected.");

                executor.close();
                TestUtils.waitForCondition(
                        () -> checkGroupState(service, GROUP, EMPTY),
                        "The group did not become empty as expected.");

                cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP, "--group", MISSING_GROUP};

                ConsumerGroupCommand.ConsumerGroupService service2 = getConsumerGroupService(cgcArgs);

                String output = ToolsTestUtils.grabConsoleOutput(service2::deleteGroups);
                assertTrue(output.contains("Group '" + MISSING_GROUP + "' could not be deleted due to:")
                                && output.contains(Errors.GROUP_ID_NOT_FOUND.message())
                                && output.contains("These consumer groups were deleted successfully: '" + GROUP + "'"),
                        "The consumer group deletion did not work as expected");
            }
        }
    }

    @ClusterTest
    public void testDeleteWithMixOfSuccessAndError() throws Exception {
        for (GroupProtocol groupProtocol : groupProtocols) {
            try (ConsumerGroupExecutor executor = buildConsumerGroupExecutor(GROUP, groupProtocol)) {
                String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP};
                ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

                TestUtils.waitForCondition(
                        () -> service.listConsumerGroups().contains(GROUP) && checkGroupState(service, GROUP, STABLE),
                        "The group did not initialize as expected.");

                executor.close();

                TestUtils.waitForCondition(
                        () -> checkGroupState(service, GROUP, EMPTY),
                        "The group did not become empty as expected.");

                cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP, "--group", MISSING_GROUP};

                ConsumerGroupCommand.ConsumerGroupService service2 = getConsumerGroupService(cgcArgs);
                Map<String, Throwable> result = service2.deleteGroups();
                assertTrue(result.size() == 2 &&
                                result.containsKey(GROUP) && result.get(GROUP) == null &&
                                result.containsKey(MISSING_GROUP) &&
                                result.get(MISSING_GROUP).getMessage().contains(Errors.GROUP_ID_NOT_FOUND.message()),
                        "The consumer group deletion did not work as expected");
            }
        }
    }

    @ClusterTest
    public void testDeleteWithUnrecognizedNewConsumerOption() {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP};
        assertThrows(OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    private ConsumerGroupExecutor buildConsumerGroupExecutor(String group, GroupProtocol protocol) {
        if (cluster.isKRaftTest()) {
            return ConsumerGroupExecutor.buildConsumerGroup(
                    cluster.bootstrapServers(),
                    1,
                    null != group ? group : GROUP,
                    TOPIC,
                    protocol.name,
                    Optional.empty(),
                    Optional.empty(),
                    false
            );
        } else {
            return ConsumerGroupExecutor.buildClassicGroup(
                    cluster.bootstrapServers(),
                    1,
                    null != group ? group : GROUP,
                    TOPIC,
                    RangeAssignor.class.getName(),
                    Optional.empty(),
                    false
            );
        }
    }

    private Predicate<String> predicateGroupState(ConsumerGroupCommand.ConsumerGroupService service, ConsumerGroupState state) {
        return groupId -> {
            try {
                return checkGroupState(service, groupId, state);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private boolean checkGroupState(ConsumerGroupCommand.ConsumerGroupService service, String groupId, ConsumerGroupState state) throws Exception {
        return Objects.equals(service.collectGroupState(groupId).state, state.toString());
    }

    ConsumerGroupCommand.ConsumerGroupService getConsumerGroupService(String[] args) {
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(args);
        return new ConsumerGroupCommand.ConsumerGroupService(
                opts,
                singletonMap(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }
}
