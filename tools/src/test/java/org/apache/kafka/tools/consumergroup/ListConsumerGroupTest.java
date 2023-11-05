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
package org.apache.kafka.tools.consumergroup;

import joptsimple.OptionException;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.consumergroup.ConsumerGroupCommand.ConsumerGroupService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);
        Set<String> expectedGroups = new HashSet<>(Arrays.asList(GROUP, simpleGroup));
        final Set[] foundGroups = new Set[]{Collections.emptySet()};
        TestUtils.waitForCondition(() -> {
            foundGroups[0] = new HashSet<>(service.listConsumerGroups());
            return Objects.equals(expectedGroups, foundGroups[0]);
        }, "Expected --list to show groups " + expectedGroups + ", but found " + foundGroups[0] + ".");
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
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Set<ConsumerGroupListing> expectedListing = new HashSet<>(Arrays.asList(
            new ConsumerGroupListing(simpleGroup, true, Optional.of(ConsumerGroupState.EMPTY)),
            new ConsumerGroupListing(GROUP, false, Optional.of(ConsumerGroupState.STABLE))));

        final Set[] foundListing = new Set[]{Collections.emptySet()};
        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listConsumerGroupsWithState(new HashSet<>(Arrays.asList(ConsumerGroupState.values()))));
            return Objects.equals(expectedListing, foundListing[0]);
        }, "Expected to show groups " + expectedListing + ", but found " + foundListing[0]);

        Set<ConsumerGroupListing> expectedListingStable = Collections.singleton(
            new ConsumerGroupListing(GROUP, false, Optional.of(ConsumerGroupState.STABLE)));

        foundListing[0] = Collections.emptySet();

        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listConsumerGroupsWithState(Collections.singleton(ConsumerGroupState.STABLE)));
            return Objects.equals(expectedListingStable, foundListing[0]);
        }, "Expected to show groups " + expectedListingStable + ", but found " + foundListing[0]);
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

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("stable"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("  bad, Stable"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("   ,   ,"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testListGroupCommand(String quorum) throws Exception {
        String simpleGroup = "simple-group";
        addSimpleGroupExecutor(simpleGroup);
        addConsumerGroupExecutor(1);
        final String[] out = {""};

        String[] cgcArgs1 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list"};
        TestUtils.waitForCondition(() -> {
            out[0] = TestUtils.grabConsoleOutput(() -> ConsumerGroupCommand.main(cgcArgs1));
            return !out[0].contains("STATE") && out[0].contains(simpleGroup) && out[0].contains(GROUP);
        }, "Expected to find " + simpleGroup + ", " + GROUP + " and no header, but found " + out[0]);

        String[] cgcArgs2 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state"};
        TestUtils.waitForCondition(() -> {
            out[0] = TestUtils.grabConsoleOutput(() -> ConsumerGroupCommand.main(cgcArgs2));
            return out[0].contains("STATE") && out[0].contains(simpleGroup) && out[0].contains(GROUP);
        }, "Expected to find " + simpleGroup + ", " + GROUP + " and the header, but found " + out[0]);

        String[] cgcArgs3 = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state", "Stable"};
        TestUtils.waitForCondition(() -> {
            out[0] = TestUtils.grabConsoleOutput(() -> ConsumerGroupCommand.main(cgcArgs3));
            return out[0].contains("STATE") && out[0].contains(GROUP) && out[0].contains("Stable");
        }, "Expected to find " + GROUP + " in state Stable and the header, but found " + out[0]);
    }
}
