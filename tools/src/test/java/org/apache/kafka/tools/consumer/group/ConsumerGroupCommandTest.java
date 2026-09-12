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

import java.util.List;
import java.util.Set;

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

    @Test
    public void testConsumerGroupStatesFromString() {
        Set<GroupState> result = ConsumerGroupCommand.groupStatesFromString("Stable");
        assertEquals(Set.of(GroupState.STABLE), result);

        result = ConsumerGroupCommand.groupStatesFromString("Stable, PreparingRebalance");
        assertEquals(Set.of(GroupState.STABLE, GroupState.PREPARING_REBALANCE), result);

        result = ConsumerGroupCommand.groupStatesFromString("Dead,CompletingRebalance,");
        assertEquals(Set.of(GroupState.DEAD, GroupState.COMPLETING_REBALANCE), result);

        result = ConsumerGroupCommand.groupStatesFromString("stable");
        assertEquals(Set.of(GroupState.STABLE), result);

        result = ConsumerGroupCommand.groupStatesFromString("stable, assigning");
        assertEquals(Set.of(GroupState.STABLE, GroupState.ASSIGNING), result);

        result = ConsumerGroupCommand.groupStatesFromString("dead,reconciling,");
        assertEquals(Set.of(GroupState.DEAD, GroupState.RECONCILING), result);

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.groupStatesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.groupStatesFromString("  bad, Stable"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.groupStatesFromString("   ,   ,"));
    }

    @Test
    public void testConsumerGroupTypesFromString() {
        Set<GroupType> result = ConsumerGroupCommand.consumerGroupTypesFromString("consumer");
        assertEquals(Set.of(GroupType.CONSUMER), result);

        result = ConsumerGroupCommand.consumerGroupTypesFromString("consumer, classic");
        assertEquals(Set.of(GroupType.CONSUMER, GroupType.CLASSIC), result);

        result = ConsumerGroupCommand.consumerGroupTypesFromString("Consumer, Classic");
        assertEquals(Set.of(GroupType.CONSUMER, GroupType.CLASSIC), result);

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("Share"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("streams"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("  bad, generic"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("   ,   ,"));
    }
}
