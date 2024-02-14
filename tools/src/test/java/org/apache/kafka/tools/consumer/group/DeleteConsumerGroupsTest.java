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
import kafka.admin.ConsumerGroupCommand;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.tools.ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeleteConsumerGroupsTest extends ConsumerGroupCommandTest {
    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteWithTopicOption(String quorum) {
        createOffsetsTopic(listenerName(), new Properties());
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--delete", "--group", GROUP, "--topic"};
        assertThrows(OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteCmdNonExistingGroup(String quorum) {
        createOffsetsTopic(listenerName(), new Properties());
        String missingGroup = "missing.group";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--delete", "--group", missingGroup};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        String output = kafka.utils.TestUtils.grabConsoleOutput(() -> {
            service.deleteGroups();
            return null;
        });
        assertTrue(output.contains("Group '" + missingGroup + "' could not be deleted due to:") && output.contains(Errors.GROUP_ID_NOT_FOUND.message()),
            "The expected error (" + Errors.GROUP_ID_NOT_FOUND + ") was not detected while deleting consumer group");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteNonExistingGroup(String quorum) {
        createOffsetsTopic(listenerName(), new Properties());
        String missingGroup = "missing.group";

        // note the group to be deleted is a different (non-existing) group
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--delete", "--group", missingGroup};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        scala.collection.Map<String, Throwable> result = service.deleteGroups();
        assertTrue(result.size() == 1 && result.contains(missingGroup) && result.get(missingGroup).get().getCause() instanceof GroupIdNotFoundException,
            "The expected error (" + Errors.GROUP_ID_NOT_FOUND + ") was not detected while deleting consumer group");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteCmdNonEmptyGroup(String quorum) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group
        addConsumerGroupExecutor(1);
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--delete", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(
            () -> service.collectGroupMembers(GROUP, false)._2.get().size() == 1,
            "The group did not initialize as expected."
        );

        String output = kafka.utils.TestUtils.grabConsoleOutput(() -> {
            service.deleteGroups();
            return null;
        });
        assertTrue(output.contains("Group '" + GROUP + "' could not be deleted due to:") && output.contains(Errors.NON_EMPTY_GROUP.message()),
            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting consumer group. Output was: (" + output + ")");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteNonEmptyGroup(String quorum) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group
        addConsumerGroupExecutor(1);
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--delete", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(
            () -> service.collectGroupMembers(GROUP, false)._2.get().size() == 1,
            "The group did not initialize as expected."
        );

        scala.collection.Map<String, Throwable> result = service.deleteGroups();
        assertNotNull(result.get(GROUP).get(),
            "Group was deleted successfully, but it shouldn't have been. Result was:(" + result + ")");
        assertTrue(result.size() == 1 && result.contains(GROUP) && result.get(GROUP).get().getCause() instanceof GroupNotEmptyException,
            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting consumer group. Result was:(" + result + ")");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteCmdEmptyGroup(String quorum) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group
        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1);
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--delete", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(
            () -> service.listConsumerGroups().contains(GROUP) && Objects.equals(service.collectGroupState(GROUP).state(), "Stable"),
            "The group did not initialize as expected."
        );

        executor.shutdown();

        TestUtils.waitForCondition(
            () -> Objects.equals(service.collectGroupState(GROUP).state(), "Empty"),
            "The group did not become empty as expected."
        );

        String output = kafka.utils.TestUtils.grabConsoleOutput(() -> {
            service.deleteGroups();
            return null;
        });
        assertTrue(output.contains("Deletion of requested consumer groups ('" + GROUP + "') was successful."),
            "The consumer group could not be deleted as expected");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteCmdAllGroups(String quorum) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // Create 3 groups with 1 consumer per each
        Map<String, ConsumerGroupExecutor> groups = IntStream.rangeClosed(1, 3).mapToObj(i -> GROUP + i).collect(Collectors.toMap(
            Function.identity(),
            group -> addConsumerGroupExecutor(1, TOPIC, group, RangeAssignor.class.getName(), Optional.empty(), Optional.empty(), false, GroupProtocol.CLASSIC.name)
        ));

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--delete", "--all-groups"};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() ->
            Objects.equals(service.listConsumerGroups().toSet(), set(groups.keySet())) &&
                groups.keySet().stream().allMatch(groupId -> {
                    try {
                        return Objects.equals(service.collectGroupState(groupId).state(), "Stable");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }),
            "The group did not initialize as expected.");

        // Shutdown consumers to empty out groups
        groups.values().forEach(AbstractConsumerGroupExecutor::shutdown);

        TestUtils.waitForCondition(() ->
            groups.keySet().stream().allMatch(groupId -> {
                try {
                    return Objects.equals(service.collectGroupState(groupId).state(), "Empty");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }),
            "The group did not become empty as expected.");

        String output = kafka.utils.TestUtils.grabConsoleOutput(() -> {
            service.deleteGroups();
            return null;
        }).trim();
        Set<String> expectedGroupsForDeletion = groups.keySet();
        Set<String> deletedGroupsGrepped = Arrays.stream(output.substring(output.indexOf('(') + 1, output.indexOf(')')).split(","))
            .map(str -> str.replaceAll("'", "").trim()).collect(Collectors.toSet());

        assertTrue(output.matches("Deletion of requested consumer groups (.*) was successful.")
            && Objects.equals(deletedGroupsGrepped, expectedGroupsForDeletion),
            "The consumer group(s) could not be deleted as expected");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteEmptyGroup(String quorum) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group
        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1);
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--delete", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(
            () -> service.listConsumerGroups().contains(GROUP) && Objects.equals(service.collectGroupState(GROUP).state(), "Stable"),
            "The group did not initialize as expected.");

        executor.shutdown();

        TestUtils.waitForCondition(
            () -> Objects.equals(service.collectGroupState(GROUP).state(), "Empty"),
            "The group did not become empty as expected.");

        scala.collection.Map<String, Throwable> result = service.deleteGroups();
        assertTrue(result.size() == 1 && result.contains(GROUP) && result.get(GROUP).get() == null,
            "The consumer group could not be deleted as expected");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteCmdWithMixOfSuccessAndError(String quorum) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());
        String missingGroup = "missing.group";

        // run one consumer in the group
        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1);
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--delete", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(
            () -> service.listConsumerGroups().contains(GROUP) && Objects.equals(service.collectGroupState(GROUP).state(), "Stable"),
            "The group did not initialize as expected.");

        executor.shutdown();

        TestUtils.waitForCondition(
            () -> Objects.equals(service.collectGroupState(GROUP).state(), "Empty"),
            "The group did not become empty as expected.");

        cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--delete", "--group", GROUP, "--group", missingGroup};

        ConsumerGroupCommand.ConsumerGroupService service2 = getConsumerGroupService(cgcArgs);

        String output = kafka.utils.TestUtils.grabConsoleOutput(() -> {
            service2.deleteGroups();
            return null;
        });
        assertTrue(output.contains("Group '" + missingGroup + "' could not be deleted due to:")
            && output.contains(Errors.GROUP_ID_NOT_FOUND.message())
            && output.contains("These consumer groups were deleted successfully: '" + GROUP + "'"),
            "The consumer group deletion did not work as expected");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteWithMixOfSuccessAndError(String quorum) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());
        String missingGroup = "missing.group";

        // run one consumer in the group
        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1);
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--delete", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(
            () -> service.listConsumerGroups().contains(GROUP) && Objects.equals(service.collectGroupState(GROUP).state(), "Stable"),
            "The group did not initialize as expected.");

        executor.shutdown();

        TestUtils.waitForCondition(
            () -> Objects.equals(service.collectGroupState(GROUP).state(), "Empty"),
            "The group did not become empty as expected.");

        cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--delete", "--group", GROUP, "--group", missingGroup};

        ConsumerGroupCommand.ConsumerGroupService service2 = getConsumerGroupService(cgcArgs);
        scala.collection.Map<String, Throwable> result = service2.deleteGroups();
        assertTrue(result.size() == 2 &&
                result.contains(GROUP) && result.get(GROUP).get() == null &&
                result.contains(missingGroup) &&
                result.get(missingGroup).get().getMessage().contains(Errors.GROUP_ID_NOT_FOUND.message()),
            "The consumer group deletion did not work as expected");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteWithUnrecognizedNewConsumerOption(String quorum) {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", bootstrapServers(listenerName()), "--delete", "--group", GROUP};
        assertThrows(OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }
}
