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
import org.apache.kafka.clients.admin.DeleteStreamsGroupsOptions;
import org.apache.kafka.clients.admin.DeleteStreamsGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
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

import java.util.ArrayList;
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
    public void testRetrieveInternalTopics() {
        Admin adminClient = mock(KafkaAdminClient.class);
        String groupId = "foo-group";
        List<String> args = new ArrayList<>(Arrays.asList("--bootstrap-server", "localhost:9092", "--group", groupId, "--delete"));
        List<String> sourceTopics = List.of("source-topic1", "source-topic2");
        List<String> repartitionSinkTopics = List.of("rep-sink-topic1", "rep-sink-topic2");
        Map<String, StreamsGroupSubtopologyDescription.TopicInfo> stateChangelogTopics = Map.of(
            groupId + "-1-changelog", mock(StreamsGroupSubtopologyDescription.TopicInfo.class),
            "some-pre-fix" + "-changelog", mock(StreamsGroupSubtopologyDescription.TopicInfo.class),
            groupId + "-2-changelog", mock(StreamsGroupSubtopologyDescription.TopicInfo.class));
        Map<String, StreamsGroupSubtopologyDescription.TopicInfo> repartitionSourceTopics = Map.of(
            groupId + "-1-repartition", mock(StreamsGroupSubtopologyDescription.TopicInfo.class),
            groupId + "-some-thing", mock(StreamsGroupSubtopologyDescription.TopicInfo.class),
            groupId + "-2-repartition", mock(StreamsGroupSubtopologyDescription.TopicInfo.class));


        Map<String, StreamsGroupDescription> resultMap = new HashMap<>();
        resultMap.put(groupId, new StreamsGroupDescription(
            groupId,
            0,
            0,
            0,
            List.of(new StreamsGroupSubtopologyDescription("subtopology1", sourceTopics, repartitionSinkTopics, stateChangelogTopics, repartitionSourceTopics)),
            List.of(),
            GroupState.DEAD,
            new Node(0, "localhost", 9092),
            null));
        DescribeStreamsGroupsResult result = mock(DescribeStreamsGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(resultMap));
        when(adminClient.describeStreamsGroups(ArgumentMatchers.anyCollection())).thenReturn(result);

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args.toArray(new String[0]), adminClient);
        Map<String, List<String>> internalTopics = service.retrieveInternalTopics(List.of(groupId));

        assertNotNull(internalTopics.get(groupId));
        assertEquals(4, internalTopics.get(groupId).size());
        assertEquals(new HashSet<>(List.of(groupId + "-1-changelog", groupId + "-2-changelog", groupId + "-1-repartition", groupId + "-2-repartition")),
            new HashSet<>(internalTopics.get(groupId)));
        assertFalse(internalTopics.get(groupId).stream().anyMatch(List.of("some-pre-fix-changelog", groupId + "-some-thing")::contains));
        assertFalse(internalTopics.get(groupId).stream().anyMatch(sourceTopics::contains));
        assertFalse(internalTopics.get(groupId).stream().anyMatch(repartitionSinkTopics::contains));

        service.close();
    }

    @Test
    public void testDeleteStreamsGroup() {
        Admin adminClient = mock(KafkaAdminClient.class);
        String groupId = "foo-group";
        List<String> args = new ArrayList<>(Arrays.asList("--bootstrap-server", "localhost:9092", "--group", groupId, "--delete"));

        DeleteStreamsGroupsResult deleteStreamsGroupsResult = mock(DeleteStreamsGroupsResult.class);
        when(adminClient.deleteStreamsGroups(eq(List.of(groupId)), any(DeleteStreamsGroupsOptions.class))).thenReturn(deleteStreamsGroupsResult);
        when(deleteStreamsGroupsResult.deletedGroups()).thenReturn(Map.of(groupId, KafkaFuture.completedFuture(null)));
        DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
        when(deleteTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(adminClient.deleteTopics(ArgumentMatchers.anyCollection())).thenReturn(deleteTopicsResult);
        DescribeStreamsGroupsResult describeStreamsGroupsResult = mock(DescribeStreamsGroupsResult.class);
        when(describeStreamsGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(groupId, mock(StreamsGroupDescription.class))));
        when(adminClient.describeStreamsGroups(any())).thenReturn(describeStreamsGroupsResult);
        ListGroupsResult listGroupsResult = mock(ListGroupsResult.class);
        when(adminClient.listGroups(any())).thenReturn(listGroupsResult);
        when(listGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(new GroupListing(groupId, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.EMPTY)))));

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args.toArray(new String[0]), adminClient);
        service.deleteGroups();

        verify(adminClient, times(1)).listGroups(any(ListGroupsOptions.class));
        verify(adminClient, times(1)).deleteStreamsGroups(eq(List.of(groupId)), any(DeleteStreamsGroupsOptions.class));
        verify(adminClient, times(1)).describeStreamsGroups(any());
        // because of having 0 internal topics, we do not expect deleteTopics to be called
        verify(adminClient, times(0)).deleteTopics(ArgumentMatchers.anyCollection());

        service.close();
    }

    @Test
    public void testDeleteNonStreamsGroup() {
        Admin adminClient = mock(KafkaAdminClient.class);
        String groupId = "foo-group";
        List<String> args = new ArrayList<>(Arrays.asList("--bootstrap-server", "localhost:9092", "--group", groupId, "--delete"));

        ListGroupsResult listGroupsResult = mock(ListGroupsResult.class);
        when(adminClient.listGroups(any())).thenReturn(listGroupsResult);
        when(listGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of()));

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args.toArray(new String[0]), adminClient);
        Map<String, Throwable> result = service.deleteGroups();

        assertNotNull(result.get(groupId));
        assertEquals(result.get(groupId).getMessage(),
            "Group '" + groupId + "' does not exist or is not a streams group.");
        assertInstanceOf(IllegalArgumentException.class, result.get(groupId));
        verify(adminClient, times(1)).listGroups(any(ListGroupsOptions.class));
        // we do not expect any further API to be called
        verify(adminClient, times(0)).deleteStreamsGroups(eq(List.of(groupId)), any(DeleteStreamsGroupsOptions.class));
        verify(adminClient, times(0)).describeStreamsGroups(any());
        verify(adminClient, times(0)).deleteTopics(ArgumentMatchers.anyCollection());

        service.close();
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
