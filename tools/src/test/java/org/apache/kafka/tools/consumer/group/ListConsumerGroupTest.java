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
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.tools.ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ListConsumerGroupTest extends ConsumerGroupCommandTest {
    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource("getTestQuorumAndGroupProtocolParametersAll")
    public void testListConsumerGroupsWithoutFilters(String quorum, String groupProtocol) throws Exception {
        String simpleGroup = "simple-group";

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
    public void testListWithUnrecognizedNewConsumerOption() throws Exception {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", bootstrapServers(listenerName()), "--list"};
        assertThrows(OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource("getTestQuorumAndGroupProtocolParametersAll")
    public void testListConsumerGroupsWithStates(String quorum, String groupProtocol) throws Exception {
        String simpleGroup = "simple-group";

        createOffsetsTopic(listenerName(), new Properties());

        addSimpleGroupExecutor(simpleGroup);
        addConsumerGroupExecutor(1);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list"};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Set<ConsumerGroupListing> expectedListing = new HashSet<>(Arrays.asList(
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
        ));

        assertGroupListing(
            Collections.emptySet(),
            EnumSet.allOf(ConsumerGroupState.class),
            expectedListing,
            service
        );

        expectedListing = new HashSet<>(Arrays.asList(
            new ConsumerGroupListing(
                GROUP,
                false,
                Optional.of(ConsumerGroupState.STABLE),
                Optional.of(GroupType.CLASSIC)
            )
        ));

        assertGroupListing(
            Collections.emptySet(),
            new HashSet<>(Arrays.asList(ConsumerGroupState.STABLE)),
            expectedListing,
            service
        );

        assertGroupListing(
            Collections.emptySet(),
            new HashSet<>(Arrays.asList(ConsumerGroupState.PREPARING_REBALANCE)),
            Collections.emptySet(),
            service
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

        Set<ConsumerGroupListing> expectedListing = new HashSet<>(Arrays.asList(
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
        ));

        // No filters explicitly mentioned. Expectation is that all groups are returned.
        assertGroupListing(Collections.emptySet(), Collections.emptySet(), expectedListing, service);

        // When group type is mentioned:
        // Old Group Coordinator returns empty listings if the type is not Classic.
        // New Group Coordinator returns groups according to the filter.
        assertGroupListing(new HashSet<>(Arrays.asList(GroupType.CONSUMER)), Collections.emptySet(), Collections.emptySet(), service);

        assertGroupListing(new HashSet<>(Arrays.asList(GroupType.CLASSIC)), Collections.emptySet(), expectedListing, service);
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
        Set<ConsumerGroupListing> expectedListing = new HashSet<>(Arrays.asList(
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
        ));

        assertGroupListing(Collections.emptySet(), Collections.emptySet(), expectedListing, service);

        // When group type is mentioned:
        // New Group Coordinator returns groups according to the filter.
        expectedListing = new HashSet<>(Arrays.asList(
            new ConsumerGroupListing(
                PROTOCOL_GROUP,
                false,
                Optional.of(ConsumerGroupState.STABLE),
                Optional.of(GroupType.CONSUMER)
            )
        ));

        assertGroupListing(new HashSet<>(Arrays.asList(GroupType.CONSUMER)), Collections.emptySet(), expectedListing, service);

        expectedListing = new HashSet<>(Arrays.asList(
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
        ));

        assertGroupListing(new HashSet<>(Arrays.asList(GroupType.CLASSIC)), Collections.emptySet(), expectedListing, service);
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
        assertEquals(set(Arrays.asList(ConsumerGroupState.STABLE)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("stable, assigning");
        assertEquals(set(Arrays.asList(ConsumerGroupState.STABLE, ConsumerGroupState.ASSIGNING)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("dead,reconciling,");
        assertEquals(set(Arrays.asList(ConsumerGroupState.DEAD, ConsumerGroupState.RECONCILING)), result);

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("  bad, Stable"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("   ,   ,"));
    }

    @Test
    public void testConsumerGroupTypesFromString() throws Exception {
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

        final AtomicReference<String> out = new AtomicReference<>("");

        String[] cgcArgs1 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list"};
        TestUtils.waitForCondition(() -> {
            out.set(kafka.utils.TestUtils.grabConsoleOutput(() -> {
                ConsumerGroupCommand.main(cgcArgs1);
                return null;
            }));
            return !out.get().contains("TYPE") && !out.get().contains("STATE") && out.get().contains(simpleGroup) && out.get().contains(GROUP);
        }, "Expected to find " + simpleGroup + ", " + GROUP + " and no header, but found " + out.get());

        String[] cgcArgs2 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state"};
        TestUtils.waitForCondition(() -> {
            out.set(kafka.utils.TestUtils.grabConsoleOutput(() -> {
                ConsumerGroupCommand.main(cgcArgs2);
                return null;
            }));
            return out.get().contains("STATE") && out.get().contains(simpleGroup) && out.get().contains(GROUP);
        }, "Expected to find " + simpleGroup + ", " + GROUP + " and the header, but found " + out.get());

        String[] cgcArgs3 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type"};
        TestUtils.waitForCondition(() -> {
            out.set(kafka.utils.TestUtils.grabConsoleOutput(() -> {
                ConsumerGroupCommand.main(cgcArgs3);
                return null;
            }));
            return out.get().contains("TYPE") && !out.get().contains("STATE") && out.get().contains(simpleGroup) && out.get().contains(GROUP);
        }, "Expected to find " + simpleGroup + ", " + GROUP + " and the header, but found " + out.get());

        String[] cgcArgs4 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state", "--type"};
        TestUtils.waitForCondition(() -> {
            out.set(kafka.utils.TestUtils.grabConsoleOutput(() -> {
                ConsumerGroupCommand.main(cgcArgs4);
                return null;
            }));
            return out.get().contains("TYPE") && out.get().contains("STATE") && out.get().contains(simpleGroup) && out.get().contains(GROUP);
        }, "Expected to find " + simpleGroup + ", " + GROUP + " and the header, but found " + out.get());

        String[] cgcArgs5 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state", "Stable"};
        TestUtils.waitForCondition(() -> {
            out.set(kafka.utils.TestUtils.grabConsoleOutput(() -> {
                ConsumerGroupCommand.main(cgcArgs5);
                return null;
            }));
            return out.get().contains("STATE") && out.get().contains(GROUP) && out.get().contains("Stable");
        }, "Expected to find " + GROUP + " in state Stable and the header, but found " + out.get());

        String[] cgcArgs6 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state", "stable"};
        TestUtils.waitForCondition(() -> {
            out.set(kafka.utils.TestUtils.grabConsoleOutput(() -> {
                ConsumerGroupCommand.main(cgcArgs6);
                return null;
            }));
            return out.get().contains("STATE") && out.get().contains(GROUP) && out.get().contains("Stable");
        }, "Expected to find " + GROUP + " in state Stable and the header, but found " + out.get());

        String[] cgcArgs7 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type", "Classic"};
        TestUtils.waitForCondition(() -> {
            out.set(kafka.utils.TestUtils.grabConsoleOutput(() -> {
                ConsumerGroupCommand.main(cgcArgs7);
                return null;
            }));
            return out.get().contains("TYPE") && out.get().contains("Classic") && !out.get().contains("STATE") &&
                out.get().contains(simpleGroup) && out.get().contains(GROUP);
        }, "Expected to find " + GROUP + " and the header, but found " + out.get());

        String[] cgcArgs8 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type", "classic"};
        TestUtils.waitForCondition(() -> {
            out.set(kafka.utils.TestUtils.grabConsoleOutput(() -> {
                ConsumerGroupCommand.main(cgcArgs8);
                return null;
            }));
            return out.get().contains("TYPE") && out.get().contains("Classic") && !out.get().contains("STATE") &&
                out.get().contains(simpleGroup) && out.get().contains(GROUP);
        }, "Expected to find " + GROUP + " and the header, but found " + out.get());
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource("getTestQuorumAndGroupProtocolParametersConsumerGroupProtocolOnly")
    public void testListGroupCommandConsumerProtocol(String quorum, String groupProtocol) throws Exception {
        String simpleGroup = "simple-group";

        createOffsetsTopic(listenerName(), new Properties());

        addSimpleGroupExecutor(simpleGroup);
        addConsumerGroupExecutor(1, PROTOCOL_GROUP, groupProtocol);

        final AtomicReference<String> out = new AtomicReference<>("");

        String[] cgcArgs1 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list"};
        TestUtils.waitForCondition(() -> {
            out.set(kafka.utils.TestUtils.grabConsoleOutput(() -> {
                ConsumerGroupCommand.main(cgcArgs1);
                return null;
            }));
            return !out.get().contains("TYPE") && !out.get().contains("STATE") &&
                out.get().contains(simpleGroup) && out.get().contains(PROTOCOL_GROUP);
        }, "Expected to find " + simpleGroup + ", " + PROTOCOL_GROUP + " and no header, but found " + out.get());

        String[] cgcArgs2 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state"};
        TestUtils.waitForCondition(() -> {
            out.set(kafka.utils.TestUtils.grabConsoleOutput(() -> {
                ConsumerGroupCommand.main(cgcArgs2);
                return null;
            }));
            return out.get().contains("STATE") && !out.get().contains("TYPE") && out.get().contains(simpleGroup) && out.get().contains(PROTOCOL_GROUP);
        }, "Expected to find " + simpleGroup + ", " + PROTOCOL_GROUP + " and state header, but found " + out.get());

        String[] cgcArgs3 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type"};
        TestUtils.waitForCondition(() -> {
            out.set(kafka.utils.TestUtils.grabConsoleOutput(() -> {
                ConsumerGroupCommand.main(cgcArgs3);
                return null;
            }));
            return out.get().contains("TYPE") && out.get().contains("Consumer") && !out.get().contains("STATE") &&
                out.get().contains(PROTOCOL_GROUP) && out.get().contains(simpleGroup);
        }, "Expected to find " + simpleGroup + ", " + PROTOCOL_GROUP + " and type header, but found " + out.get());

        String[] cgcArgs4 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type", "consumer"};
        TestUtils.waitForCondition(() -> {
            out.set(kafka.utils.TestUtils.grabConsoleOutput(() -> {
                ConsumerGroupCommand.main(cgcArgs4);
                return null;
            }));
            return out.get().contains("TYPE") && out.get().contains("Consumer") && !out.get().contains("STATE") && out.get().contains(PROTOCOL_GROUP);
        }, "Expected to find " + PROTOCOL_GROUP + " with type header and Consumer type, but found " + out.get());

        String[] cgcArgs5 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--type", "consumer", "--state", "Stable"};
        TestUtils.waitForCondition(() -> {
            out.set(kafka.utils.TestUtils.grabConsoleOutput(() -> {
                ConsumerGroupCommand.main(cgcArgs5);
                return null;
            }));
            return out.get().contains("TYPE") && out.get().contains("Consumer") && out.get().contains("STATE") &&
                out.get().contains("Stable") && out.get().contains(PROTOCOL_GROUP);
        }, "Expected to find " + PROTOCOL_GROUP + " with type and state header, but found " + out.get());
    }

    private void assertGroupListing(
        Set<GroupType> typeFilterSet,
        Set<ConsumerGroupState> stateFilterSet,
        Set<ConsumerGroupListing> expectedListing,
        ConsumerGroupCommand.ConsumerGroupService service
    ) throws Exception {
        final AtomicReference<scala.collection.Set> foundListing = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            foundListing.set(service.listConsumerGroupsWithFilters(set(typeFilterSet), set(stateFilterSet)).toSet());
            return Objects.equals(set(expectedListing), foundListing.get());
        }, "Expected to show groups " + expectedListing + ", but found " + foundListing.get() + ".");
    }
}
