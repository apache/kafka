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
package org.apache.kafka.tools.streams;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupMemberAssignment;
import org.apache.kafka.clients.admin.StreamsGroupMemberDescription;
import org.apache.kafka.clients.admin.StreamsGroupSubtopologyDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import joptsimple.OptionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamsGroupCommandTest {

    @Test
    public void testListStreamsGroups() throws Exception {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--list"};
        Admin adminClient = mock(KafkaAdminClient.class);
        ListGroupsResult result = mock(ListGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Arrays.asList(
            new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE)),
            new GroupListing(secondGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.EMPTY))
        )));
        when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(result);
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(cgcArgs, adminClient);
        Set<String> expectedGroups = new HashSet<>(Arrays.asList(firstGroup, secondGroup));

        final Set[] foundGroups = new Set[]{Set.of()};
        TestUtils.waitForCondition(() -> {
            foundGroups[0] = new HashSet<>(service.listStreamsGroups());
            return Objects.equals(expectedGroups, foundGroups[0]);
        }, "Expected --list to show groups " + expectedGroups + ", but found " + foundGroups[0] + ".");
        service.close();
    }

    @Test
    public void testListWithUnrecognizedOption() {
        String bootstrapServer = "localhost:9092";
        String[] cgcArgs = new String[]{"--frivolous-nonsense", "--bootstrap-server", bootstrapServer, "--list"};
        final Exception exception = assertThrows(OptionException.class, () -> {
            getStreamsGroupService(cgcArgs, new MockAdminClient());
        });
        assertEquals("frivolous-nonsense is not a recognized option", exception.getMessage());
    }

    @Test
    public void testListStreamsGroupsWithStates() throws Exception {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--list", "--state"};
        Admin adminClient = mock(KafkaAdminClient.class);
        ListGroupsResult resultWithAllStates = mock(ListGroupsResult.class);
        when(resultWithAllStates.all()).thenReturn(KafkaFuture.completedFuture(Arrays.asList(
            new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE)),
            new GroupListing(secondGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.EMPTY))
        )));
        when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(resultWithAllStates);
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(cgcArgs, adminClient);
        Set<GroupListing> expectedListing = new HashSet<>(Arrays.asList(
            new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE)),
            new GroupListing(secondGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.EMPTY))));

        final Set[] foundListing = new Set[]{Set.of()};
        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listStreamsGroupsInStates(new HashSet<>(Arrays.asList(GroupState.values()))));
            return Objects.equals(expectedListing, foundListing[0]);
        }, "Expected to show groups " + expectedListing + ", but found " + foundListing[0]);

        ListGroupsResult resultWithStableState = mock(ListGroupsResult.class);
        when(resultWithStableState.all()).thenReturn(KafkaFuture.completedFuture(List.of(
            new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE))
        )));
        when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(resultWithStableState);
        Set<GroupListing> expectedListingStable = Set.of(
            new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE)));

        foundListing[0] = Set.of();

        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listStreamsGroupsInStates(Set.of(GroupState.STABLE)));
            return Objects.equals(expectedListingStable, foundListing[0]);
        }, "Expected to show groups " + expectedListingStable + ", but found " + foundListing[0]);
        service.close();
    }

    @Test
    public void testDescribeStreamsGroups() throws Exception {
        String firstGroup = "group1";
        Admin adminClient = mock(KafkaAdminClient.class);
        DescribeStreamsGroupsResult result = mock(DescribeStreamsGroupsResult.class);
        Map<String, StreamsGroupDescription> resultMap = new HashMap<>();
        StreamsGroupDescription exp = new StreamsGroupDescription(
            firstGroup,
            0,
            0,
            0,
            List.of(new StreamsGroupSubtopologyDescription("foo", List.of(), List.of(), Map.of(), Map.of())),
            List.of(),
            GroupState.STABLE,
            new Node(0, "bar", 0),
            null);
        resultMap.put(firstGroup, exp);

        when(result.all()).thenReturn(KafkaFuture.completedFuture(resultMap));
        when(adminClient.describeStreamsGroups(ArgumentMatchers.anyCollection())).thenReturn(result);
        StreamsGroupCommand.StreamsGroupService service = new StreamsGroupCommand.StreamsGroupService(null, adminClient);
        assertEquals(exp, service.getDescribeGroup(firstGroup));
        service.close();
    }

    @Test
    public void testDescribeStreamsGroupsGetOffsets() throws Exception {
        Admin adminClient = mock(KafkaAdminClient.class);

        ListOffsetsResult startOffset = mock(ListOffsetsResult.class);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> startOffsetResultMap = new HashMap<>();
        startOffsetResultMap.put(new TopicPartition("topic1", 0), new ListOffsetsResult.ListOffsetsResultInfo(10, -1, Optional.empty()));

        ListOffsetsResult endOffset = mock(ListOffsetsResult.class);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsetResultMap = new HashMap<>();
        endOffsetResultMap.put(new TopicPartition("topic1", 0), new ListOffsetsResult.ListOffsetsResultInfo(30, -1, Optional.empty()));

        when(startOffset.all()).thenReturn(KafkaFuture.completedFuture(startOffsetResultMap));
        when(endOffset.all()).thenReturn(KafkaFuture.completedFuture(endOffsetResultMap));

        when(adminClient.listOffsets(ArgumentMatchers.anyMap())).thenReturn(startOffset, endOffset);

        ListConsumerGroupOffsetsResult result = mock(ListConsumerGroupOffsetsResult.class);
        Map<TopicPartition, OffsetAndMetadata> committedOffsetsMap = new HashMap<>();
        committedOffsetsMap.put(new TopicPartition("topic1", 0), new OffsetAndMetadata(12, Optional.of(0), ""));

        when(adminClient.listConsumerGroupOffsets(ArgumentMatchers.anyMap())).thenReturn(result);
        when(result.partitionsToOffsetAndMetadata(ArgumentMatchers.anyString())).thenReturn(KafkaFuture.completedFuture(committedOffsetsMap));

        StreamsGroupMemberDescription description = new StreamsGroupMemberDescription("foo", 0, Optional.empty(),
            Optional.empty(), "bar", "baz", 0, "qux",
            Optional.empty(), Map.of(), List.of(), List.of(),
            new StreamsGroupMemberAssignment(List.of(), List.of(), List.of()), new StreamsGroupMemberAssignment(List.of(), List.of(), List.of()),
            false);
        StreamsGroupDescription x = new StreamsGroupDescription(
            "group1",
            0,
            0,
            0,
            List.of(new StreamsGroupSubtopologyDescription("id", List.of("topic1"), List.of(), Map.of(), Map.of())),
            List.of(description),
            GroupState.STABLE,
            new Node(0, "host", 0),
            null);
        StreamsGroupCommand.StreamsGroupService service = new StreamsGroupCommand.StreamsGroupService(null, adminClient);
        Map<TopicPartition, StreamsGroupCommand.OffsetsInfo> lags = service.getOffsets(x);
        assertEquals(1, lags.size());
        assertEquals(new StreamsGroupCommand.OffsetsInfo(Optional.of(12L), Optional.of(0), 30L, 18L), lags.get(new TopicPartition("topic1", 0)));
        service.close();
    }

    @Test
    public void testPrintEmptyGroupState() {
        assertFalse(StreamsGroupCommand.StreamsGroupService.isGroupStateValid(GroupState.EMPTY, 0));
        assertFalse(StreamsGroupCommand.StreamsGroupService.isGroupStateValid(GroupState.DEAD, 0));
        assertFalse(StreamsGroupCommand.StreamsGroupService.isGroupStateValid(GroupState.STABLE, 0));
        assertTrue(StreamsGroupCommand.StreamsGroupService.isGroupStateValid(GroupState.STABLE, 1));
        assertTrue(StreamsGroupCommand.StreamsGroupService.isGroupStateValid(GroupState.UNKNOWN, 1));
    }

    @Test
    public void testGroupStatesFromString() {
        Set<GroupState> result = StreamsGroupCommand.groupStatesFromString("empty");
        assertEquals(new HashSet<>(List.of(GroupState.EMPTY)), result);
        result = StreamsGroupCommand.groupStatesFromString("EMPTY");
        assertEquals(new HashSet<>(List.of(GroupState.EMPTY)), result);

        result = StreamsGroupCommand.groupStatesFromString("notready");
        assertEquals(new HashSet<>(List.of(GroupState.NOT_READY)), result);
        result = StreamsGroupCommand.groupStatesFromString("notReady");
        assertEquals(new HashSet<>(List.of(GroupState.NOT_READY)), result);

        result = StreamsGroupCommand.groupStatesFromString("assigning");
        assertEquals(new HashSet<>(List.of(GroupState.ASSIGNING)), result);
        result = StreamsGroupCommand.groupStatesFromString("ASSIGNING");
        assertEquals(new HashSet<>(List.of(GroupState.ASSIGNING)), result);

        result = StreamsGroupCommand.groupStatesFromString("RECONCILING");
        assertEquals(new HashSet<>(List.of(GroupState.RECONCILING)), result);
        result = StreamsGroupCommand.groupStatesFromString("reconCILING");
        assertEquals(new HashSet<>(List.of(GroupState.RECONCILING)), result);

        result = StreamsGroupCommand.groupStatesFromString("STABLE");
        assertEquals(new HashSet<>(List.of(GroupState.STABLE)), result);
        result = StreamsGroupCommand.groupStatesFromString("stable");
        assertEquals(new HashSet<>(List.of(GroupState.STABLE)), result);

        result = StreamsGroupCommand.groupStatesFromString("DEAD");
        assertEquals(new HashSet<>(List.of(GroupState.DEAD)), result);
        result = StreamsGroupCommand.groupStatesFromString("dead");
        assertEquals(new HashSet<>(List.of(GroupState.DEAD)), result);

        assertThrow("preparingRebalance");
        assertThrow("completingRebalance");
        assertThrow("bad, wrong");
        assertThrow("  bad, Stable");
        assertThrow("   ,   ,");
    }

    StreamsGroupCommand.StreamsGroupService getStreamsGroupService(String[] args, Admin adminClient) {
        StreamsGroupCommandOptions opts = new StreamsGroupCommandOptions(args);
        return new StreamsGroupCommand.StreamsGroupService(opts, adminClient);
    }

    private static void assertThrow(final String wrongState) {
        final Set<String> validStates = new HashSet<>(Arrays.asList("Assigning", "Dead", "Empty", "Reconciling", "Stable", "NotReady"));

        final Exception exception = assertThrows(IllegalArgumentException.class, () -> StreamsGroupCommand.groupStatesFromString(wrongState));

        assertTrue(exception.getMessage().contains(" Valid states are: "));

        final String[] exceptionMessage = exception.getMessage().split(" Valid states are: ");
        assertEquals("Invalid state list '" + wrongState + "'.", exceptionMessage[0]);
        assertEquals(Arrays.stream(exceptionMessage[1].split(","))
            .map(String::trim)
            .collect(Collectors.toSet()), validStates);
    }
}
