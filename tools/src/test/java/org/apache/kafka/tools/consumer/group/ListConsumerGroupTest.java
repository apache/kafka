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
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.tools.ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ListConsumerGroupTest extends ConsumerGroupCommandTest {
    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource("getTestQuorumAndGroupProtocolParametersAll")
    public void testListConsumerGroupsWithoutFilters(String quorum, String groupProtocol) throws Exception {
        String simpleGroup = "simple-group";

        createOffsetsTopic(listenerName(), new Properties());

        addSimpleGroupExecutor(simpleGroup);
        addConsumerGroupExecutor(1);
        addConsumerGroupExecutor(1, PROTOCOL_GROUP, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list"};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        scala.collection.Set<String> expectedGroups = set(Arrays.asList(GROUP, simpleGroup, PROTOCOL_GROUP));
        final AtomicReference<scala.collection.Set> foundGroups = new AtomicReference<>();

        TestUtils.waitForCondition(() -> {
            foundGroups.set(service.listConsumerGroups().toSet());
            return Objects.equals(expectedGroups, foundGroups.get());
        }, "Expected --list to show groups " + expectedGroups + ", but found " + foundGroups.get() + ".");
    }

    @Test
    public void testListWithUnrecognizedNewConsumerOption() {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", bootstrapServers(listenerName()), "--list"};
        assertThrows(OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource("getTestQuorumAndGroupProtocolParametersAll")
    public void testListConsumerGroupsWithStates(String quorum, String groupProtocol) throws Exception {
        String simpleGroup = "simple-group";

        createOffsetsTopic(listenerName(), new Properties());

        addSimpleGroupExecutor(simpleGroup);
        addConsumerGroupExecutor(1, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state"};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Set<ConsumerGroupListing> expectedListing = mkSet(
            new ConsumerGroupListing(
                simpleGroup,
                true,
                Optional.of(ConsumerGroupState.EMPTY),
                Optional.of(GroupType.CLASSIC)
            ),
            new ConsumerGroupListing(
                GROUP,
                false,
                Optional.of(ConsumerGroupState.STABLE),
                Optional.of(GroupType.parse(groupProtocol))
            )
        );

        assertGroupListing(
            service,
            Collections.emptySet(),
            EnumSet.allOf(ConsumerGroupState.class),
            expectedListing
        );

        expectedListing = mkSet(
            new ConsumerGroupListing(
                GROUP,
                false,
                Optional.of(ConsumerGroupState.STABLE),
                Optional.of(GroupType.parse(groupProtocol))
            )
        );

        assertGroupListing(
            service,
            Collections.emptySet(),
            mkSet(ConsumerGroupState.STABLE),
            expectedListing
        );

        assertGroupListing(
            service,
            Collections.emptySet(),
            mkSet(ConsumerGroupState.PREPARING_REBALANCE),
            Collections.emptySet()
        );
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly")
    public void testListConsumerGroupsWithTypesClassicProtocol(String quorum, String groupProtocol) throws Exception {
        String simpleGroup = "simple-group";

        createOffsetsTopic(listenerName(), new Properties());

        addSimpleGroupExecutor(simpleGroup);
        addConsumerGroupExecutor(1);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list"};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Set<ConsumerGroupListing> expectedListing = mkSet(
            new ConsumerGroupListing(
                simpleGroup,
                true,
                Optional.of(ConsumerGroupState.EMPTY),
                Optional.of(GroupType.CLASSIC)
            ),
            new ConsumerGroupListing(
                GROUP,
                false,
                Optional.of(ConsumerGroupState.STABLE),
                Optional.of(GroupType.CLASSIC)
            )
        );

        // No filters explicitly mentioned. Expectation is that all groups are returned.
        assertGroupListing(
            service,
            Collections.emptySet(),
            Collections.emptySet(),
            expectedListing
        );

        // When group type is mentioned:
        // Old Group Coordinator returns empty listings if the type is not Classic.
        // New Group Coordinator returns groups according to the filter.
        assertGroupListing(
            service,
            mkSet(GroupType.CONSUMER),
            Collections.emptySet(),
            Collections.emptySet()
        );

        assertGroupListing(
            service,
            mkSet(GroupType.CLASSIC),
            Collections.emptySet(),
            expectedListing
        );
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource("getTestQuorumAndGroupProtocolParametersConsumerGroupProtocolOnly")
    public void testListConsumerGroupsWithTypesConsumerProtocol(String quorum, String groupProtocol) throws Exception {
        String simpleGroup = "simple-group";

        createOffsetsTopic(listenerName(), new Properties());

        addSimpleGroupExecutor(simpleGroup);
        addConsumerGroupExecutor(1);
        addConsumerGroupExecutor(1, PROTOCOL_GROUP, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list"};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        // No filters explicitly mentioned. Expectation is that all groups are returned.
        Set<ConsumerGroupListing> expectedListing = mkSet(
            new ConsumerGroupListing(
                simpleGroup,
                true,
                Optional.of(ConsumerGroupState.EMPTY),
                Optional.of(GroupType.CLASSIC)
            ),
            new ConsumerGroupListing(
                GROUP,
                false,
                Optional.of(ConsumerGroupState.STABLE),
                Optional.of(GroupType.CLASSIC)
            ),
            new ConsumerGroupListing(
                PROTOCOL_GROUP,
                false,
                Optional.of(ConsumerGroupState.STABLE),
                Optional.of(GroupType.CONSUMER)
            )
        );

        assertGroupListing(
            service,
            Collections.emptySet(),
            Collections.emptySet(),
            expectedListing
        );

        // When group type is mentioned:
        // New Group Coordinator returns groups according to the filter.
        expectedListing = mkSet(
            new ConsumerGroupListing(
                PROTOCOL_GROUP,
                false,
                Optional.of(ConsumerGroupState.STABLE),
                Optional.of(GroupType.CONSUMER)
            )
        );

        assertGroupListing(
            service,
            mkSet(GroupType.CONSUMER),
            Collections.emptySet(),
            expectedListing
        );

        expectedListing = mkSet(
            new ConsumerGroupListing(
                simpleGroup,
                true,
                Optional.of(ConsumerGroupState.EMPTY),
                Optional.of(GroupType.CLASSIC)
            ),
            new ConsumerGroupListing(
                GROUP,
                false,
                Optional.of(ConsumerGroupState.STABLE),
                Optional.of(GroupType.CLASSIC)
            )
        );

        assertGroupListing(
            service,
            mkSet(GroupType.CLASSIC),
            Collections.emptySet(),
            expectedListing
        );
    }

    @Test
    public void testConsumerGroupStatesFromString() {
        scala.collection.Set<ConsumerGroupState> result = ConsumerGroupCommand.consumerGroupStatesFromString("Stable");
        assertEquals(set(Collections.singleton(ConsumerGroupState.STABLE)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("Stable, PreparingRebalance");
        assertEquals(set(Arrays.asList(ConsumerGroupState.STABLE, ConsumerGroupState.PREPARING_REBALANCE)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("Dead,CompletingRebalance,");
        assertEquals(set(Arrays.asList(ConsumerGroupState.DEAD, ConsumerGroupState.COMPLETING_REBALANCE)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("stable");
        assertEquals(set(Collections.singletonList(ConsumerGroupState.STABLE)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("stable, assigning");
        assertEquals(set(Arrays.asList(ConsumerGroupState.STABLE, ConsumerGroupState.ASSIGNING)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("dead,reconciling,");
        assertEquals(set(Arrays.asList(ConsumerGroupState.DEAD, ConsumerGroupState.RECONCILING)), result);

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("  bad, Stable"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("   ,   ,"));
    }

    @Test
    public void testConsumerGroupTypesFromString() {
        scala.collection.Set<GroupType> result = ConsumerGroupCommand.consumerGroupTypesFromString("consumer");
        assertEquals(set(Collections.singleton(GroupType.CONSUMER)), result);

        result = ConsumerGroupCommand.consumerGroupTypesFromString("consumer, classic");
        assertEquals(set(Arrays.asList(GroupType.CONSUMER, GroupType.CLASSIC)), result);

        result = ConsumerGroupCommand.consumerGroupTypesFromString("Consumer, Classic");
        assertEquals(set(Arrays.asList(GroupType.CONSUMER, GroupType.CLASSIC)), result);

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("  bad, generic"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("   ,   ,"));
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly")
    public void testListGroupCommandClassicProtocol(String quorum, String groupProtocol) throws Exception {
        String simpleGroup = "simple-group";

        createOffsetsTopic(listenerName(), new Properties());

        addSimpleGroupExecutor(simpleGroup);
        addConsumerGroupExecutor(1);

        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list"),
            Collections.emptyList(),
            mkSet(
                Collections.singletonList(GROUP),
                Collections.singletonList(simpleGroup)
            )
        );

        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state"),
            Arrays.asList("GROUP", "STATE"),
            mkSet(
                Arrays.asList(GROUP, "Stable"),
                Arrays.asList(simpleGroup, "Empty")
            )
        );

        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type"),
            Arrays.asList("GROUP", "TYPE"),
            mkSet(
                Arrays.asList(GROUP, "Classic"),
                Arrays.asList(simpleGroup, "Classic")
            )
        );

        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type", "--state"),
            Arrays.asList("GROUP", "TYPE", "STATE"),
            mkSet(
                Arrays.asList(GROUP, "Classic", "Stable"),
                Arrays.asList(simpleGroup, "Classic", "Empty")
            )
        );

        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state", "Stable"),
            Arrays.asList("GROUP", "STATE"),
            mkSet(
                Arrays.asList(GROUP, "Stable")
            )
        );

        // Check case-insensitivity in state filter.
        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state", "stable"),
            Arrays.asList("GROUP", "STATE"),
            mkSet(
                Arrays.asList(GROUP, "Stable")
            )
        );

        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type", "Classic"),
            Arrays.asList("GROUP", "TYPE"),
            mkSet(
                Arrays.asList(GROUP, "Classic"),
                Arrays.asList(simpleGroup, "Classic")
            )
        );

        // Check case-insensitivity in type filter.
        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type", "classic"),
            Arrays.asList("GROUP", "TYPE"),
            mkSet(
                Arrays.asList(GROUP, "Classic"),
                Arrays.asList(simpleGroup, "Classic")
            )
        );
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource("getTestQuorumAndGroupProtocolParametersConsumerGroupProtocolOnly")
    public void testListGroupCommandConsumerProtocol(String quorum, String groupProtocol) throws Exception {
        String simpleGroup = "simple-group";

        createOffsetsTopic(listenerName(), new Properties());

        addSimpleGroupExecutor(simpleGroup);
        addConsumerGroupExecutor(1, PROTOCOL_GROUP, groupProtocol);

        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list"),
            Collections.emptyList(),
            mkSet(
                Collections.singletonList(PROTOCOL_GROUP),
                Collections.singletonList(simpleGroup)
            )
        );

        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state"),
            Arrays.asList("GROUP", "STATE"),
            mkSet(
                Arrays.asList(PROTOCOL_GROUP, "Stable"),
                Arrays.asList(simpleGroup, "Empty")
            )
        );

        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type"),
            Arrays.asList("GROUP", "TYPE"),
            mkSet(
                Arrays.asList(PROTOCOL_GROUP, "Consumer"),
                Arrays.asList(simpleGroup, "Classic")
            )
        );

        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type", "--state"),
            Arrays.asList("GROUP", "TYPE", "STATE"),
            mkSet(
                Arrays.asList(PROTOCOL_GROUP, "Consumer", "Stable"),
                Arrays.asList(simpleGroup, "Classic", "Empty")
            )
        );

        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type", "consumer"),
            Arrays.asList("GROUP", "TYPE"),
            mkSet(
                Arrays.asList(PROTOCOL_GROUP, "Consumer")
            )
        );

        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type", "consumer", "--state", "Stable"),
            Arrays.asList("GROUP", "TYPE", "STATE"),
            mkSet(
                Arrays.asList(PROTOCOL_GROUP, "Consumer", "Stable")
            )
        );
    }

    /**
     * Validates the consumer group listings returned against expected values using specified filters.
     *
     * @param service           The service to list consumer groups.
     * @param typeFilterSet     Filters for group types, empty for no filter.
     * @param stateFilterSet    Filters for group states, empty for no filter.
     * @param expectedListing   Expected consumer group listings.
     */
    private static void assertGroupListing(
        ConsumerGroupCommand.ConsumerGroupService service,
        Set<GroupType> typeFilterSet,
        Set<ConsumerGroupState> stateFilterSet,
        Set<ConsumerGroupListing> expectedListing
    ) throws Exception {
        final AtomicReference<scala.collection.Set> foundListing = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            foundListing.set(service.listConsumerGroupsWithFilters(set(typeFilterSet), set(stateFilterSet)).toSet());
            return Objects.equals(set(expectedListing), foundListing.get());
        }, () -> "Expected to show groups " + expectedListing + ", but found " + foundListing.get() + ".");
    }

    /**
     * Validates that the output of the list command corresponds to the expected values.
     *
     * @param args              The arguments for the command line tool.
     * @param expectedHeader    The expected header as a list of strings; or an empty list
     *                          if a header is not expected.
     * @param expectedRows      The expected rows as a set of list of columns.
     * @throws InterruptedException
     */
    private static void validateListOutput(
        List<String> args,
        List<String> expectedHeader,
        Set<List<String>> expectedRows
    ) throws InterruptedException {
        final AtomicReference<String> out = new AtomicReference<>("");
        TestUtils.waitForCondition(() -> {
            String output = runAndGrabConsoleOutput(args);
            out.set(output);

            int index = 0;
            String[] lines = output.split("\n");

            // Parse the header if one is expected.
            if (!expectedHeader.isEmpty()) {
                if (lines.length == 0) return false;
                List<String> header = Arrays.stream(lines[index++].split("\\s+")).collect(Collectors.toList());
                if (!expectedHeader.equals(header)) {
                    return false;
                }
            }

            // Parse the groups.
            Set<List<String>> groups = new HashSet<>();
            for (; index < lines.length; index++) {
                groups.add(Arrays.stream(lines[index].split("\\s+")).collect(Collectors.toList()));
            }
            return expectedRows.equals(groups);
        }, () -> String.format("Expected header=%s and groups=%s, but found:%n%s", expectedHeader, expectedRows, out.get()));
    }

    private static String runAndGrabConsoleOutput(
        List<String> args
    ) {
        return kafka.utils.TestUtils.grabConsoleOutput(() -> {
            ConsumerGroupCommand.main(args.toArray(new String[0]));
            return null;
        });
    }
}
