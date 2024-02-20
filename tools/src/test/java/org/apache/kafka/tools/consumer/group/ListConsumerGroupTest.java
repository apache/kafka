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
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ListConsumerGroupTest extends ConsumerGroupCommandTest {
    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testListConsumerGroups(String quorum) throws Exception {
        String simpleGroup = "simple-group";
        addSimpleGroupExecutor(simpleGroup);
        addConsumerGroupExecutor(1);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list"};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);
        Set<String> expectedGroups = new HashSet<>(Arrays.asList(GROUP, simpleGroup));
        final AtomicReference<Set<String>> foundGroups = new AtomicReference<>(Collections.emptySet());
        TestUtils.waitForCondition(() -> {
            foundGroups.set(new HashSet<>(service.listConsumerGroups()));
            return Objects.equals(expectedGroups, foundGroups.get());
        }, "Expected --list to show groups " + expectedGroups + ", but found " + foundGroups.get() + ".");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testListWithUnrecognizedNewConsumerOption() {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", bootstrapServers(listenerName()), "--list"};
        assertThrows(OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testListConsumerGroupsWithStates() throws Exception {
        String simpleGroup = "simple-group";
        addSimpleGroupExecutor(simpleGroup);
        addConsumerGroupExecutor(1);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state"};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Set<ConsumerGroupListing> expectedListing = new HashSet<>(Arrays.asList(
            new ConsumerGroupListing(simpleGroup, true, Optional.of(ConsumerGroupState.EMPTY)),
            new ConsumerGroupListing(GROUP, false, Optional.of(ConsumerGroupState.STABLE))));

        final AtomicReference<Set<ConsumerGroupListing>> foundListing = new AtomicReference<>(Collections.emptySet());
        TestUtils.waitForCondition(() -> {
            foundListing.set(new HashSet<>(service.listConsumerGroupsWithState(new HashSet<>(Arrays.asList(ConsumerGroupState.values())))));
            return Objects.equals(expectedListing, foundListing.get());
        }, "Expected to show groups " + expectedListing + ", but found " + foundListing.get());

        Set<ConsumerGroupListing> expectedListingStable = Collections.singleton(
            new ConsumerGroupListing(GROUP, false, Optional.of(ConsumerGroupState.STABLE)));

        foundListing.set(Collections.emptySet());

        TestUtils.waitForCondition(() -> {
            foundListing.set(new HashSet<>(service.listConsumerGroupsWithState(Collections.singleton(ConsumerGroupState.STABLE))));
            return Objects.equals(expectedListingStable, foundListing.get());
        }, "Expected to show groups " + expectedListingStable + ", but found " + foundListing.get());
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testConsumerGroupStatesFromString(String quorum) {
        Set<ConsumerGroupState> result = ConsumerGroupCommand.consumerGroupStatesFromString("Stable");
        assertEquals(Collections.singleton(ConsumerGroupState.STABLE), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("Stable, PreparingRebalance");
        assertEquals(new HashSet<>(Arrays.asList(ConsumerGroupState.STABLE, ConsumerGroupState.PREPARING_REBALANCE)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("Dead,CompletingRebalance,");
        assertEquals(new HashSet<>(Arrays.asList(ConsumerGroupState.DEAD, ConsumerGroupState.COMPLETING_REBALANCE)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("stable");
        assertEquals(new HashSet<>(Arrays.asList(ConsumerGroupState.STABLE)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("stable, assigning");
        assertEquals(new HashSet<>(Arrays.asList(ConsumerGroupState.STABLE, ConsumerGroupState.ASSIGNING)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("dead,reconciling,");
        assertEquals(new HashSet<>(Arrays.asList(ConsumerGroupState.DEAD, ConsumerGroupState.RECONCILING)), result);

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("  bad, Stable"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("   ,   ,"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testListGroupCommand(String quorum) throws Exception {
        String simpleGroup = "simple-group";
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
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state", "Stable"),
            Arrays.asList("GROUP", "STATE"),
            mkSet(
                Arrays.asList(GROUP, "Stable")
            )
        );

        validateListOutput(
            Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state", "stable"),
            Arrays.asList("GROUP", "STATE"),
            mkSet(
                Arrays.asList(GROUP, "Stable")
            )
        );
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
