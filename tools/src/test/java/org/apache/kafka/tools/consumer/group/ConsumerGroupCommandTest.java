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

import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.tools.ToolsTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import joptsimple.OptionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConsumerGroupCommandTest {
    private static final String DUMMY_BOOTSTRAP_SERVERS = "localhost:9092";

    @Test
    public void testValidateRegexCommandWithValidRegex() {
        String output = ToolsTestUtils.grabConsoleOutput(
            () -> ConsumerGroupCommand.main(List.of(
                "--validate-regex",
                "foo.*"
            ).toArray(new String[0]))
        );

        assertEquals(
            "The regular expression `foo.*` is valid.\n",
            output
        );
    }

    @Test
    public void testValidateRegexCommandWithInvalidRegex() {
        String output = ToolsTestUtils.grabConsoleOutput(
            () -> ConsumerGroupCommand.main(List.of(
                "--validate-regex",
                "[foo.*"
            ).toArray(new String[0]))
        );

        assertEquals(
            "The regular expression `[foo.*` is invalid: missing closing ].\n",
            output
        );
    }

    @Test
    public void testListWithUnrecognizedNewConsumerOption() {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", DUMMY_BOOTSTRAP_SERVERS, "--list"};
        assertThrows(OptionException.class, () -> ConsumerGroupCommandOptions.fromArgs(cgcArgs));
    }

    private static Stream<Arguments> validConsumerGroupStates() {
        return Stream.of(
                Arguments.of("Stable", Set.of(GroupState.STABLE)),
                Arguments.of("Stable, PreparingRebalance", Set.of(GroupState.STABLE, GroupState.PREPARING_REBALANCE)),
                Arguments.of("Dead,CompletingRebalance,", Set.of(GroupState.DEAD, GroupState.COMPLETING_REBALANCE)),
                Arguments.of("stable", Set.of(GroupState.STABLE)),
                Arguments.of("stable, assigning", Set.of(GroupState.STABLE, GroupState.ASSIGNING)),
                Arguments.of("dead,reconciling,", Set.of(GroupState.DEAD, GroupState.RECONCILING))
        );
    }

    @ParameterizedTest
    @MethodSource("validConsumerGroupStates")
    public void testConsumerGroupStatesFromString(String input, Set<GroupState> expected) {
        assertEquals(expected, ConsumerGroupCommand.groupStatesFromString(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"bad, wrong", "  bad, Stable", "   ,   ,"})
    public void testConsumerGroupStatesFromInvalidString(String input) {
        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.groupStatesFromString(input));
    }

    private static Stream<Arguments> validConsumerGroupTypes() {
        return Stream.of(
                Arguments.of("consumer", Set.of(GroupType.CONSUMER)),
                Arguments.of("consumer, classic", Set.of(GroupType.CONSUMER, GroupType.CLASSIC)),
                Arguments.of("Consumer, Classic", Set.of(GroupType.CONSUMER, GroupType.CLASSIC))
        );
    }

    @ParameterizedTest
    @MethodSource("validConsumerGroupTypes")
    public void testConsumerGroupTypesFromString(String input, Set<GroupType> expected) {
        assertEquals(expected, ConsumerGroupCommand.consumerGroupTypesFromString(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Share", "streams", "bad, wrong", "  bad, generic", "   ,   ,"})
    public void testConsumerGroupTypesFromInvalidString(String input) {
        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString(input));
    }
}
